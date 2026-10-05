from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from execledger.models import (
    AttemptRecord,
    EffectRecord,
    ExecutionRecord,
    ExecutionSpec,
    ExecutionStatus,
    SnapshotRecord,
    utc_now,
)

CURRENT_SCHEMA_VERSION = 3


class IdempotencyConflict(ValueError):
    pass


class InvalidTransition(ValueError):
    pass


class LostLease(RuntimeError):
    pass


@dataclass(frozen=True)
class ClaimedExecution:
    execution: ExecutionRecord
    lease_token: str
    attempt_id: str


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
        self._conn = sqlite3.connect(
            db_path,
            check_same_thread=False,
            isolation_level=None,
            timeout=30,
        )
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        self._init_schema()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _table_exists(self, name: str) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (name,),
        ).fetchone()
        return row is not None

    def _column_names(self, table: str) -> set[str]:
        return {row["name"] for row in self._conn.execute(f"PRAGMA table_info({table})")}

    def _create_current_schema(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE executions (
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
                cancel_requested INTEGER NOT NULL DEFAULT 0,
                worker_id TEXT,
                lease_token TEXT,
                lease_expires_at TEXT,
                next_attempt_at TEXT
            );
            CREATE TABLE idempotency (
                key TEXT PRIMARY KEY,
                request_hash TEXT NOT NULL,
                execution_id TEXT NOT NULL REFERENCES executions(id)
            );
            CREATE TABLE effects (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                execution_id TEXT NOT NULL REFERENCES executions(id),
                created_at TEXT NOT NULL,
                kind TEXT NOT NULL,
                payload_json TEXT NOT NULL
            );
            CREATE INDEX idx_effects_execution
              ON effects(execution_id, seq);
            CREATE TABLE snapshots (
                id TEXT PRIMARY KEY,
                execution_id TEXT NOT NULL REFERENCES executions(id),
                phase TEXT NOT NULL,
                created_at TEXT NOT NULL,
                digest TEXT NOT NULL,
                manifest_json TEXT NOT NULL
            );
            CREATE INDEX idx_snapshots_execution
              ON snapshots(execution_id, created_at);
            CREATE TABLE attempts (
                id TEXT PRIMARY KEY,
                execution_id TEXT NOT NULL REFERENCES executions(id),
                number INTEGER NOT NULL,
                worker_id TEXT NOT NULL,
                lease_token TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                exit_code INTEGER,
                UNIQUE(execution_id, number)
            );
            CREATE INDEX idx_attempts_execution
              ON attempts(execution_id, number);
            PRAGMA user_version = 3;
            """
        )

    def _migrate_v1_to_v2(self) -> None:
        columns = self._column_names("executions")
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            if "worker_id" not in columns:
                self._conn.execute("ALTER TABLE executions ADD COLUMN worker_id TEXT")
            if "lease_token" not in columns:
                self._conn.execute("ALTER TABLE executions ADD COLUMN lease_token TEXT")
            if "lease_expires_at" not in columns:
                self._conn.execute("ALTER TABLE executions ADD COLUMN lease_expires_at TEXT")
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS attempts (
                    id TEXT PRIMARY KEY,
                    execution_id TEXT NOT NULL REFERENCES executions(id),
                    number INTEGER NOT NULL,
                    worker_id TEXT NOT NULL,
                    lease_token TEXT NOT NULL,
                    status TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    exit_code INTEGER,
                    UNIQUE(execution_id, number)
                )
                """
            )
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_attempts_execution
                ON attempts(execution_id, number)
                """
            )
            self._conn.execute("PRAGMA user_version = 2")
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def _migrate_v2_to_v3(self) -> None:
        columns = self._column_names("executions")
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            if "next_attempt_at" not in columns:
                self._conn.execute(
                    "ALTER TABLE executions ADD COLUMN next_attempt_at TEXT"
                )
            self._conn.execute("PRAGMA user_version = 3")
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            version = int(self._conn.execute("PRAGMA user_version").fetchone()[0])
            if version > CURRENT_SCHEMA_VERSION:
                raise RuntimeError(
                    f"database schema {version} is newer than supported "
                    f"{CURRENT_SCHEMA_VERSION}"
                )
            if not self._table_exists("executions"):
                self._create_current_schema()
                return
            if version < 2:
                self._migrate_v1_to_v2()
                version = 2
            if version < 3:
                self._migrate_v2_to_v3()

    @property
    def schema_version(self) -> int:
        with self._lock:
            return int(self._conn.execute("PRAGMA user_version").fetchone()[0])

    @staticmethod
    def _canonical_spec(spec: ExecutionSpec) -> str:
        return json.dumps(spec.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))

    @classmethod
    def _request_hash(cls, spec: ExecutionSpec) -> str:
        return hashlib.sha256(cls._canonical_spec(spec).encode()).hexdigest()

    def create_execution(
        self, spec: ExecutionSpec, idempotency_key: str
    ) -> tuple[ExecutionRecord, bool]:
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
                        prior = self._conn.execute(
                            "SELECT spec_json FROM executions WHERE id = ?",
                            (existing["execution_id"],),
                        ).fetchone()
                        if prior is None:
                            raise RuntimeError("idempotency mapping references missing execution")
                        prior_spec = ExecutionSpec.model_validate_json(prior["spec_json"])
                        if prior_spec.model_dump(mode="json") != spec.model_dump(mode="json"):
                            raise IdempotencyConflict(
                                "idempotency key reused with a different request"
                            )
                        self._conn.execute(
                            "UPDATE idempotency SET request_hash = ? WHERE key = ?",
                            (request_hash, idempotency_key),
                        )
                        self._conn.execute(
                            "UPDATE executions SET request_hash = ? WHERE id = ?",
                            (request_hash, existing["execution_id"]),
                        )
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
            worker_id=row["worker_id"],
            lease_expires_at=row["lease_expires_at"],
            next_attempt_at=row["next_attempt_at"],
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

    def claim_next(
        self,
        worker_id: str,
        *,
        lease_seconds: float = 5.0,
        now: datetime | None = None,
    ) -> ClaimedExecution | None:
        if not worker_id:
            raise ValueError("worker_id must be non-empty")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")

        now_dt = now or utc_now()
        now_text = now_dt.isoformat()
        expires = (now_dt + timedelta(seconds=lease_seconds)).isoformat()
        lease_token = uuid.uuid4().hex
        attempt_id = uuid.uuid4().hex

        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    """
                    SELECT id, attempt FROM executions
                    WHERE status = ? AND cancel_requested = 0
                      AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
                    ORDER BY created_at ASC LIMIT 1
                    """,
                    (ExecutionStatus.QUEUED.value, now_text),
                ).fetchone()
                if row is None:
                    self._conn.execute("COMMIT")
                    return None

                execution_id = row["id"]
                attempt_number = int(row["attempt"]) + 1
                changed = self._conn.execute(
                    """
                    UPDATE executions
                    SET status = ?, started_at = COALESCE(started_at, ?),
                        updated_at = ?, attempt = ?, worker_id = ?,
                        lease_token = ?, lease_expires_at = ?,
                        next_attempt_at = NULL
                    WHERE id = ? AND status = ?
                    """,
                    (
                        ExecutionStatus.RUNNING.value,
                        now_text,
                        now_text,
                        attempt_number,
                        worker_id,
                        lease_token,
                        expires,
                        execution_id,
                        ExecutionStatus.QUEUED.value,
                    ),
                ).rowcount
                if changed != 1:
                    raise RuntimeError("atomic claim lost unexpectedly")

                self._conn.execute(
                    """
                    INSERT INTO attempts(
                      id, execution_id, number, worker_id, lease_token,
                      status, started_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        attempt_id,
                        execution_id,
                        attempt_number,
                        worker_id,
                        lease_token,
                        ExecutionStatus.RUNNING.value,
                        now_text,
                    ),
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

        self.add_effect(
            execution_id,
            "execution_claimed",
            {
                "worker_id": worker_id,
                "attempt": attempt_number,
                "lease_expires_at": expires,
            },
        )
        return ClaimedExecution(
            execution=self.get(execution_id),
            lease_token=lease_token,
            attempt_id=attempt_id,
        )

    def renew_lease(
        self,
        execution_id: str,
        lease_token: str,
        *,
        lease_seconds: float = 5.0,
    ) -> bool:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        now_dt = utc_now()
        expires = (now_dt + timedelta(seconds=lease_seconds)).isoformat()
        now = now_dt.isoformat()
        with self._lock:
            changed = self._conn.execute(
                """
                UPDATE executions
                SET lease_expires_at = ?, updated_at = ?
                WHERE id = ? AND status = ? AND lease_token = ?
                  AND lease_expires_at > ?
                """,
                (
                    expires,
                    now,
                    execution_id,
                    ExecutionStatus.RUNNING.value,
                    lease_token,
                    now,
                ),
            ).rowcount
        return changed == 1

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
                      finished_at = ?, next_attempt_at = NULL
                    WHERE id = ? AND status = ?
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

    @staticmethod
    def _retry_plan(
        spec: ExecutionSpec,
        outcome: ExecutionStatus,
        attempt_number: int,
        cancel_requested: bool,
        now: datetime,
    ) -> tuple[str, float] | None:
        policy = spec.retry_policy
        if cancel_requested:
            return None
        if outcome not in policy.retry_on:
            return None
        if attempt_number >= policy.max_attempts:
            return None
        delay = policy.delay_after_attempt(attempt_number)
        next_attempt_at = (now + timedelta(seconds=delay)).isoformat()
        return next_attempt_at, delay

    def finish(
        self,
        execution_id: str,
        status: ExecutionStatus,
        *,
        exit_code: int | None,
        stdout: str,
        stderr: str,
        lease_token: str | None = None,
        now: datetime | None = None,
    ) -> ExecutionRecord:
        now_dt = now or utc_now()
        now_text = now_dt.isoformat()
        retry_payload: dict[str, object] | None = None

        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    """
                    SELECT status, attempt, lease_token, lease_expires_at,
                           spec_json, cancel_requested
                    FROM executions WHERE id = ?
                    """,
                    (execution_id,),
                ).fetchone()
                if row is None:
                    raise KeyError(execution_id)

                current_status = ExecutionStatus(row["status"])
                if status not in _ALLOWED_TRANSITIONS.get(current_status, set()):
                    raise InvalidTransition(f"{current_status} -> {status}")

                current_token = row["lease_token"]
                expires = row["lease_expires_at"]
                if current_status == ExecutionStatus.RUNNING and current_token is not None:
                    if lease_token != current_token:
                        raise LostLease(execution_id)
                    if expires is not None and expires <= now_text:
                        raise LostLease(execution_id)

                attempt_number = int(row["attempt"])
                spec = ExecutionSpec.model_validate_json(row["spec_json"])
                retry = self._retry_plan(
                    spec,
                    status,
                    attempt_number,
                    bool(row["cancel_requested"]),
                    now_dt,
                )

                if attempt_number > 0:
                    self._conn.execute(
                        """
                        UPDATE attempts
                        SET status = ?, finished_at = ?, exit_code = ?
                        WHERE execution_id = ? AND number = ?
                        """,
                        (
                            status.value,
                            now_text,
                            exit_code,
                            execution_id,
                            attempt_number,
                        ),
                    )

                if retry is not None:
                    next_attempt_at, delay = retry
                    self._conn.execute(
                        """
                        UPDATE executions
                        SET status = ?, updated_at = ?, finished_at = NULL,
                            exit_code = ?, stdout = ?, stderr = ?,
                            worker_id = NULL, lease_token = NULL,
                            lease_expires_at = NULL, next_attempt_at = ?
                        WHERE id = ?
                        """,
                        (
                            ExecutionStatus.QUEUED.value,
                            now_text,
                            exit_code,
                            stdout,
                            stderr,
                            next_attempt_at,
                            execution_id,
                        ),
                    )
                    retry_payload = {
                        "attempt": attempt_number,
                        "outcome": status.value,
                        "next_attempt": attempt_number + 1,
                        "next_attempt_at": next_attempt_at,
                        "delay_seconds": delay,
                    }
                else:
                    self._conn.execute(
                        """
                        UPDATE executions
                        SET status = ?, updated_at = ?, finished_at = ?,
                            exit_code = ?, stdout = ?, stderr = ?,
                            worker_id = NULL, lease_token = NULL,
                            lease_expires_at = NULL, next_attempt_at = NULL
                        WHERE id = ?
                        """,
                        (
                            status.value,
                            now_text,
                            now_text,
                            exit_code,
                            stdout,
                            stderr,
                            execution_id,
                        ),
                    )

                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

        if retry_payload is not None:
            self.add_effect(execution_id, "retry_scheduled", retry_payload)
        else:
            self.add_effect(
                execution_id,
                "execution_finished",
                {"status": status.value, "exit_code": exit_code},
            )
        return self.get(execution_id)

    def expire_leases(self, now: datetime | None = None) -> list[str]:
        now_dt = now or utc_now()
        now_text = now_dt.isoformat()
        expired: list[
            tuple[str, str | None, int, dict[str, object] | None]
        ] = []

        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                rows = self._conn.execute(
                    """
                    SELECT id, worker_id, attempt, spec_json, cancel_requested
                    FROM executions
                    WHERE status = ?
                      AND (lease_expires_at IS NULL OR lease_expires_at <= ?)
                    """,
                    (ExecutionStatus.RUNNING.value, now_text),
                ).fetchall()

                for row in rows:
                    execution_id = row["id"]
                    attempt_number = int(row["attempt"])
                    spec = ExecutionSpec.model_validate_json(row["spec_json"])
                    retry = self._retry_plan(
                        spec,
                        ExecutionStatus.INTERRUPTED,
                        attempt_number,
                        bool(row["cancel_requested"]),
                        now_dt,
                    )

                    if attempt_number > 0:
                        self._conn.execute(
                            """
                            UPDATE attempts
                            SET status = ?, finished_at = ?
                            WHERE execution_id = ? AND number = ? AND status = ?
                            """,
                            (
                                ExecutionStatus.INTERRUPTED.value,
                                now_text,
                                execution_id,
                                attempt_number,
                                ExecutionStatus.RUNNING.value,
                            ),
                        )

                    retry_payload: dict[str, object] | None = None
                    if retry is not None:
                        next_attempt_at, delay = retry
                        self._conn.execute(
                            """
                            UPDATE executions
                            SET status = ?, updated_at = ?, finished_at = NULL,
                                worker_id = NULL, lease_token = NULL,
                                lease_expires_at = NULL, next_attempt_at = ?
                            WHERE id = ? AND status = ?
                            """,
                            (
                                ExecutionStatus.QUEUED.value,
                                now_text,
                                next_attempt_at,
                                execution_id,
                                ExecutionStatus.RUNNING.value,
                            ),
                        )
                        retry_payload = {
                            "attempt": attempt_number,
                            "outcome": ExecutionStatus.INTERRUPTED.value,
                            "next_attempt": attempt_number + 1,
                            "next_attempt_at": next_attempt_at,
                            "delay_seconds": delay,
                        }
                    else:
                        self._conn.execute(
                            """
                            UPDATE executions
                            SET status = ?, updated_at = ?, finished_at = ?,
                                worker_id = NULL, lease_token = NULL,
                                lease_expires_at = NULL, next_attempt_at = NULL
                            WHERE id = ? AND status = ?
                            """,
                            (
                                ExecutionStatus.INTERRUPTED.value,
                                now_text,
                                now_text,
                                execution_id,
                                ExecutionStatus.RUNNING.value,
                            ),
                        )

                    expired.append(
                        (
                            execution_id,
                            row["worker_id"],
                            attempt_number,
                            retry_payload,
                        )
                    )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

        for execution_id, worker_id, attempt_number, retry_payload in expired:
            self.add_effect(
                execution_id,
                "lease_expired",
                {
                    "worker_id": worker_id,
                    "attempt": attempt_number,
                },
            )
            if retry_payload is not None:
                self.add_effect(execution_id, "retry_scheduled", retry_payload)
            else:
                self.add_effect(
                    execution_id,
                    "execution_finished",
                    {
                        "status": ExecutionStatus.INTERRUPTED.value,
                        "exit_code": None,
                    },
                )
        return [execution_id for execution_id, _, _, _ in expired]

    def attempts(self, execution_id: str) -> list[AttemptRecord]:
        self.get(execution_id)
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT id, execution_id, number, worker_id, status,
                       started_at, finished_at, exit_code
                FROM attempts
                WHERE execution_id = ?
                ORDER BY number ASC
                """,
                (execution_id,),
            ).fetchall()
        return [
            AttemptRecord(
                id=row["id"],
                execution_id=row["execution_id"],
                number=row["number"],
                worker_id=row["worker_id"],
                status=ExecutionStatus(row["status"]),
                started_at=row["started_at"],
                finished_at=row["finished_at"],
                exit_code=row["exit_code"],
            )
            for row in rows
        ]

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
        return self.effects_after(execution_id)

    def effects_after(
        self,
        execution_id: str,
        after_seq: int = 0,
        *,
        limit: int = 1000,
    ) -> list[EffectRecord]:
        self.get(execution_id)
        limit = max(1, min(limit, 5000))
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT * FROM effects
                WHERE execution_id = ? AND seq > ?
                ORDER BY seq ASC
                LIMIT ?
                """,
                (execution_id, after_seq, limit),
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

    def get_snapshot(self, execution_id: str, snapshot_id: str) -> SnapshotRecord:
        self.get(execution_id)
        with self._lock:
            row = self._conn.execute(
                """
                SELECT * FROM snapshots
                WHERE execution_id = ? AND id = ?
                """,
                (execution_id, snapshot_id),
            ).fetchone()
        if row is None:
            raise KeyError(snapshot_id)
        return SnapshotRecord(
            id=row["id"],
            execution_id=row["execution_id"],
            phase=row["phase"],
            created_at=row["created_at"],
            digest=row["digest"],
            manifest=json.loads(row["manifest_json"]),
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
