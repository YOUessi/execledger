import json
import sqlite3
import time
from datetime import timedelta
from pathlib import Path

import pytest

from execledger.models import ExecutionSpec, ExecutionStatus, utc_now
from execledger.store import (
    ExecutionStore,
    IdempotencyConflict,
    LostLease,
)


def test_idempotent_submit_returns_same_execution(tmp_path: Path):
    store = ExecutionStore(tmp_path / "db.sqlite3")
    spec = ExecutionSpec(argv=["python", "-c", "print('ok')"])
    first, created = store.create_execution(spec, "key-1")
    second, created_again = store.create_execution(spec, "key-1")
    assert created is True
    assert created_again is False
    assert first.id == second.id
    store.close()


def test_idempotency_key_rejects_payload_change(tmp_path: Path):
    store = ExecutionStore(tmp_path / "db.sqlite3")
    store.create_execution(ExecutionSpec(argv=["echo", "a"]), "same-key")
    with pytest.raises(IdempotencyConflict):
        store.create_execution(ExecutionSpec(argv=["echo", "b"]), "same-key")
    store.close()


def test_claim_finish_and_attempt_history(tmp_path: Path):
    store = ExecutionStore(tmp_path / "db.sqlite3")
    record, _ = store.create_execution(ExecutionSpec(argv=["echo", "ok"]), "k")
    claim = store.claim_next("worker-a", lease_seconds=5)
    assert claim is not None
    assert claim.execution.id == record.id
    assert claim.execution.status == ExecutionStatus.RUNNING
    assert claim.execution.attempt == 1
    assert claim.execution.worker_id == "worker-a"
    assert claim.execution.lease_expires_at is not None

    done = store.finish(
        record.id,
        ExecutionStatus.SUCCEEDED,
        exit_code=0,
        stdout="ok\n",
        stderr="",
        lease_token=claim.lease_token,
    )
    assert done.status == ExecutionStatus.SUCCEEDED
    assert done.exit_code == 0
    assert done.worker_id is None
    assert done.lease_expires_at is None

    attempts = store.attempts(record.id)
    assert len(attempts) == 1
    assert attempts[0].number == 1
    assert attempts[0].worker_id == "worker-a"
    assert attempts[0].status == ExecutionStatus.SUCCEEDED

    effects = store.effects(record.id)
    assert [effect.kind for effect in effects] == [
        "execution_submitted",
        "execution_claimed",
        "execution_finished",
    ]
    store.close()


def test_second_store_cannot_claim_valid_lease(tmp_path: Path):
    db = tmp_path / "db.sqlite3"
    first = ExecutionStore(db)
    second = ExecutionStore(db)
    record, _ = first.create_execution(ExecutionSpec(argv=["echo", "ok"]), "lease-key")

    claim = first.claim_next("worker-a", lease_seconds=5)
    assert claim is not None
    assert second.claim_next("worker-b", lease_seconds=5) is None
    assert first.renew_lease(record.id, claim.lease_token, lease_seconds=5) is True

    first.close()
    second.close()


def test_wrong_lease_token_cannot_finish(tmp_path: Path):
    store = ExecutionStore(tmp_path / "db.sqlite3")
    record, _ = store.create_execution(ExecutionSpec(argv=["echo", "ok"]), "fence-key")
    claim = store.claim_next("worker-a", lease_seconds=5)
    assert claim is not None

    with pytest.raises(LostLease):
        store.finish(
            record.id,
            ExecutionStatus.SUCCEEDED,
            exit_code=0,
            stdout="",
            stderr="",
            lease_token="stale-token",
        )

    assert store.get(record.id).status == ExecutionStatus.RUNNING
    store.close()


def test_expired_lease_becomes_interrupted(tmp_path: Path):
    db = tmp_path / "db.sqlite3"
    first = ExecutionStore(db)
    second = ExecutionStore(db)
    record, _ = first.create_execution(ExecutionSpec(argv=["echo", "ok"]), "expire-key")
    claim = first.claim_next("worker-a", lease_seconds=5)
    assert claim is not None

    expired = second.expire_leases(now=utc_now() + timedelta(seconds=10))
    assert expired == [record.id]

    final = first.get(record.id)
    assert final.status == ExecutionStatus.INTERRUPTED
    assert final.worker_id is None
    assert first.renew_lease(record.id, claim.lease_token, lease_seconds=5) is False
    attempts = first.attempts(record.id)
    assert attempts[0].status == ExecutionStatus.INTERRUPTED
    assert first.effects(record.id)[-1].kind == "lease_expired"

    first.close()
    second.close()


def test_heartbeat_after_deadline_is_rejected(tmp_path: Path):
    store = ExecutionStore(tmp_path / "db.sqlite3")
    record, _ = store.create_execution(ExecutionSpec(argv=["echo", "ok"]), "late-heartbeat")
    claim = store.claim_next("worker-a", lease_seconds=0.01)
    assert claim is not None
    time.sleep(0.03)
    assert store.renew_lease(record.id, claim.lease_token, lease_seconds=1) is False
    store.close()


def test_effects_after_resumes_from_sequence(tmp_path: Path):
    store = ExecutionStore(tmp_path / "db.sqlite3")
    record, _ = store.create_execution(ExecutionSpec(argv=["echo", "ok"]), "effects-key")
    store.add_effect(record.id, "first", {"value": 1})
    store.add_effect(record.id, "second", {"value": 2})
    effects = store.effects(record.id)
    cursor = next(effect.seq for effect in effects if effect.kind == "first")

    resumed = store.effects_after(record.id, cursor)
    assert [effect.kind for effect in resumed] == ["second"]
    assert resumed[0].payload == {"value": 2}
    store.close()


def test_v01_database_migrates_in_place(tmp_path: Path):
    db = tmp_path / "legacy.sqlite3"
    conn = sqlite3.connect(db)
    conn.executescript(
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
            cancel_requested INTEGER NOT NULL DEFAULT 0
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
        CREATE TABLE snapshots (
            id TEXT PRIMARY KEY,
            execution_id TEXT NOT NULL REFERENCES executions(id),
            phase TEXT NOT NULL,
            created_at TEXT NOT NULL,
            digest TEXT NOT NULL,
            manifest_json TEXT NOT NULL
        );
        """
    )
    spec = ExecutionSpec(argv=["echo", "legacy"])
    now = utc_now().isoformat()
    conn.execute(
        """
        INSERT INTO executions(
            id, request_hash, spec_json, status, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            "legacy-execution",
            "request-hash",
            json.dumps(spec.model_dump(mode="json")),
            ExecutionStatus.QUEUED.value,
            now,
            now,
        ),
    )
    conn.commit()
    conn.close()

    store = ExecutionStore(db)
    assert store.schema_version == 2
    migrated = store.get("legacy-execution")
    assert migrated.status == ExecutionStatus.QUEUED
    assert migrated.worker_id is None
    assert migrated.lease_expires_at is None
    assert store.attempts("legacy-execution") == []
    store.close()
