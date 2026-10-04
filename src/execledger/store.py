from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import uuid
from pathlib import Path

from execledger.models import (
    EffectRecord,
    ExecutionRecord,
    ExecutionSpec,
    ExecutionStatus,
    SnapshotRecord,
    utc_now,
)


class IdempotencyConflict(ValueError):
    pass


class InvalidTransition(ValueError):
    pass


_ALLOWED_TRANSITIONS: dict[ExecutionStatus, set[ExecutionStatus]] = {
    ExecutionStatus.QUEUED: {ExecutionStatus.RUNNING, ExecutionStatus.CANCELLED},
    ExecutionStatus.RUNNING: {
        ExecutionStatus.SUCCEEDED,
        ExecutionStatus.FAILED,
        ExecutionStatus.TIMED_OUT,
        ExecutionStatus.CANCELLED,
        ExecutionStatus.INTERRUPTED,
    },
}


class ExecutionStore:
    def __init__(self, db_path: Path):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(db_path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        self._init_schema()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                PRAGMA journal_mode=WAL;
                PRAGMA foreign_keys=ON;
                CREATE TABLE IF NOT EXISTS executions (
                    id TEXT PRIMARY KEY,
                    request_hash TEXT NOT NULL,
                    spec_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    exit_code INTEGER,
                    stdout TEXT NOT NULL DEFAULT '',
                    stderr TEXT NOT NULL DEFAULT '',
                    attempt INTEGER NOT NULL DEFAULT 0,
                    cancel_requested INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS idempotency (
                    key TEXT PRIMARY KEY,
                    request_hash TEXT NOT NULL,
                    execution_id TEXT NOT NULL REFERENCES executions(id)
                );
                CREATE TABLE IF NOT EXISTS effects (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    execution_id TEXT NOT NULL REFERENCES executions(id),
                    created_at TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_effects_execution
                  ON effects(execution_id, seq);
                CREATE TABLE IF NOT EXISTS snapshots (
                    id TEXT PRIMARY KEY,
                    execution_id TEXT NOT NULL REFERENCES executions(id),
                    phase TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    digest TEXT NOT NULL,
                    manifest_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_snapshots_execution
                  ON snapshots(execution_id, created_at);
                """
            )

    @staticmethod
    def _canonical_spec(spec: ExecutionSpec) -> str:
        return json.dumps(spec.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))

    @classmethod
    def _request_hash(cls, spec: ExecutionSpec) -> str:
        return hashlib.sha256(cls._canonical_spec(spec).encode()).hexdigest()

    def create_execution(self, spec: ExecutionSpec, idempotency_key: str) -> tuple[ExecutionRecord, bool]:
        if not idempotency_key or len(idempotency_key) > 200:
            raise ValueError("idempotency key must be 1..200 characters")
        request_hash = self._request_hash(spec)
        now = utc_now().isoformat()
        execution_id = uuid.uuid4().hex
        spec_json = self._canonical_spec(spec)
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                existing = self._conn.execute(
                    "SELECT request_hash, execution_id FROM idempotency WHERE key = ?",
                    (idempotency_key,),
                ).fetchone()
                if existing is not None:
                    if existing["request_hash"] != request_hash:
                        raise IdempotencyConflict("idempotency key reused with a different request")
                    self._conn.execute("COMMIT")
                    return self.get(existing["execution_id"]), False
                self._conn.execute(
                    """
                    INSERT INTO executions(
                      id, request_hash, spec_json, status, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        execution_id,
                        request_hash,
                        spec_json,
                        ExecutionStatus.QUEUED.value,
                        now,
                        now,
                    ),
                )
                self._conn.execute(
                    "INSERT INTO idempotency(key, request_hash, execution_id) VALUES (?, ?, ?)",
                    (idempotency_key, request_hash, execution_id),
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        self.add_effect(execution_id, "execution_submitted", {"request_hash": request_hash})
        return self.get(execution_id), True

    def _row_to_execution(self, row: sqlite3.Row) -> ExecutionRecord:
        return ExecutionRecord(
            id=row["id"],
            status=ExecutionStatus(row["status"]),
            spec=ExecutionSpec.model_validate_json(row["spec_json"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            exit_code=row["exit_code"],
            stdout=row["stdout"],
            stderr=row["stderr"],
            attempt=row["attempt"],
            cancel_requested=bool(row["cancel_requested"]),
        )

    def get(self, execution_id: str) -> ExecutionRecord:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM executions WHERE id = ?", (execution_id,)
            ).fetchone()
        if row is None:
            raise KeyError(execution_id)
        return self._row_to_execution(row)

    def list(self, limit: int = 100) -> list[ExecutionRecord]:
        limit = max(1, min(limit, 500))
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM executions ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return [self._row_to_execution(row) for row in rows]

    def claim_next(self) -> ExecutionRecord | None:
        now = utc_now().isoformat()
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    """
                    SELECT id FROM executions
                    WHERE status = ? AND cancel_requested = 0
                    ORDER BY created_at ASC LIMIT 1
                    """,
                    (ExecutionStatus.QUEUED.value,),
                ).fetchone()
                if row is None:
                    self._conn.execute("COMMIT")
                    return None
                execution_id = row["id"]
                self._conn.execute(
                    """
                    UPDATE executions
                    SET status = ?, started_at = COALESCE(started_at, ?),
                        updated_at = ?, attempt = attempt + 1
                    WHERE id = ? AND status = ?
                    """,
                    (
                        ExecutionStatus.RUNNING.value,
                        now,
                        now,
                        execution_id,
                        ExecutionStatus.QUEUED.value,
                    ),
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        self.add_effect(execution_id, "execution_claimed", {})
        return self.get(execution_id)

    def request_cancel(self, execution_id: str) -> ExecutionRecord:
        current = self.get(execution_id)
        if current.status in {
            ExecutionStatus.SUCCEEDED,
            ExecutionStatus.FAILED,
            ExecutionStatus.TIMED_OUT,
            ExecutionStatus.CANCELLED,
            ExecutionStatus.INTERRUPTED,
        }:
            return current
        now = utc_now().isoformat()
        with self._lock:
            if current.status == ExecutionStatus.QUEUED:
                self._conn.execute(
                    """
                    UPDATE executions SET cancel_requested = 1, status = ?, updated_at = ?,
                      finished_at = ? WHERE id = ? AND status = ?
                    """,
                    (
                        ExecutionStatus.CANCELLED.value,
                        now,
                        now,
                        execution_id,
                        ExecutionStatus.QUEUED.value,
                    ),
                )
            else:
                self._conn.execute(
                    "UPDATE executions SET cancel_requested = 1, updated_at = ? WHERE id = ?",
                    (now, execution_id),
                )
        self.add_effect(execution_id, "cancel_requested", {})
        return self.get(execution_id)

    def finish(
        self,
        execution_id: str,
        status: ExecutionStatus,
        *,
        exit_code: int | None,
        stdout: str,
        stderr: str,
    ) -> ExecutionRecord:
        current = self.get(execution_id)
        if status not in _ALLOWED_TRANSITIONS.get(current.status, set()):
            raise InvalidTransition(f"{current.status} -> {status}")
        now = utc_now().isoformat()
        with self._lock:
            self._conn.execute(
                """
                UPDATE executions SET status = ?, updated_at = ?, finished_at = ?,
                  exit_code = ?, stdout = ?, stderr = ? WHERE id = ?
                """,
                (status.value, now, now, exit_code, stdout, stderr, execution_id),
            )
        self.add_effect(
            execution_id,
            "execution_finished",
            {"status": status.value, "exit_code": exit_code},
        )
        return self.get(execution_id)

    def recover_running(self) -> list[str]:
        now = utc_now().isoformat()
        with self._lock:
            rows = self._conn.execute(
                "SELECT id FROM executions WHERE status = ?",
                (ExecutionStatus.RUNNING.value,),
            ).fetchall()
            ids = [row["id"] for row in rows]
            if ids:
                self._conn.execute(
                    """
                    UPDATE executions SET status = ?, updated_at = ?, finished_at = ?
                    WHERE status = ?
                    """,
                    (
                        ExecutionStatus.INTERRUPTED.value,
                        now,
                        now,
                        ExecutionStatus.RUNNING.value,
                    ),
                )
        for execution_id in ids:
            self.add_effect(execution_id, "recovered_as_interrupted", {})
        return ids

    def add_effect(self, execution_id: str, kind: str, payload: dict[str, object]) -> None:
        now = utc_now().isoformat()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO effects(execution_id, created_at, kind, payload_json)
                VALUES (?, ?, ?, ?)
                """,
                (execution_id, now, kind, json.dumps(payload, sort_keys=True)),
            )

    def effects(self, execution_id: str) -> list[EffectRecord]:
        self.get(execution_id)
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM effects WHERE execution_id = ? ORDER BY seq ASC",
                (execution_id,),
            ).fetchall()
        return [
            EffectRecord(
                seq=row["seq"],
                execution_id=row["execution_id"],
                created_at=row["created_at"],
                kind=row["kind"],
                payload=json.loads(row["payload_json"]),
            )
            for row in rows
        ]

    def add_snapshot(
        self,
        execution_id: str,
        phase: str,
        digest: str,
        manifest: list[dict[str, object]],
    ) -> SnapshotRecord:
        snapshot_id = uuid.uuid4().hex
        now = utc_now().isoformat()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO snapshots(id, execution_id, phase, created_at, digest, manifest_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot_id,
                    execution_id,
                    phase,
                    now,
                    digest,
                    json.dumps(manifest, sort_keys=True),
                ),
            )
        self.add_effect(execution_id, "snapshot_created", {"phase": phase, "digest": digest})
        return SnapshotRecord(
            id=snapshot_id,
            execution_id=execution_id,
            phase=phase,
            created_at=now,
            digest=digest,
            manifest=manifest,
        )

    def snapshots(self, execution_id: str) -> list[SnapshotRecord]:
        self.get(execution_id)
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM snapshots WHERE execution_id = ? ORDER BY created_at ASC",
                (execution_id,),
            ).fetchall()
        return [
            SnapshotRecord(
                id=row["id"],
                execution_id=row["execution_id"],
                phase=row["phase"],
                created_at=row["created_at"],
                digest=row["digest"],
                manifest=json.loads(row["manifest_json"]),
            )
            for row in rows
        ]
