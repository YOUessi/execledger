import hashlib
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
    effects = first.effects(record.id)
    assert any(effect.kind == "lease_expired" for effect in effects)
    assert effects[-1].kind == "execution_finished"

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
    legacy_payload = spec.model_dump(mode="json")
    legacy_payload.pop("retry_policy")
    legacy_json = json.dumps(legacy_payload, sort_keys=True, separators=(",", ":"))
    legacy_hash = hashlib.sha256(legacy_json.encode()).hexdigest()
    now = utc_now().isoformat()
    conn.execute(
        """
        INSERT INTO executions(
            id, request_hash, spec_json, status, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            "legacy-execution",
            legacy_hash,
            legacy_json,
            ExecutionStatus.QUEUED.value,
            now,
            now,
        ),
    )
    conn.execute(
        """
        INSERT INTO idempotency(key, request_hash, execution_id)
        VALUES (?, ?, ?)
        """,
        ("legacy-key", legacy_hash, "legacy-execution"),
    )
    conn.commit()
    conn.close()

    store = ExecutionStore(db)
    assert store.schema_version == 3
    migrated = store.get("legacy-execution")
    assert migrated.status == ExecutionStatus.QUEUED
    assert migrated.worker_id is None
    assert migrated.lease_expires_at is None
    assert migrated.next_attempt_at is None
    assert store.attempts("legacy-execution") == []

    replayed, created = store.create_execution(spec, "legacy-key")
    assert created is False
    assert replayed.id == "legacy-execution"
    store.close()


def test_failed_attempt_requeues_until_retry_budget_is_exhausted(tmp_path: Path):
    store = ExecutionStore(tmp_path / "db.sqlite3")
    base = utc_now()
    spec = ExecutionSpec(
        argv=["echo", "retry"],
        retry_policy={
            "max_attempts": 2,
            "retry_on": ["FAILED"],
            "backoff_initial_seconds": 5,
            "backoff_multiplier": 2,
            "backoff_max_seconds": 30,
        },
    )
    record, _ = store.create_execution(spec, "retry-budget")

    first = store.claim_next("worker-a", lease_seconds=60, now=base)
    assert first is not None
    queued = store.finish(
        record.id,
        ExecutionStatus.FAILED,
        exit_code=7,
        stdout="",
        stderr="first failure",
        lease_token=first.lease_token,
        now=base + timedelta(seconds=1),
    )
    assert queued.status == ExecutionStatus.QUEUED
    assert queued.attempt == 1
    assert queued.next_attempt_at == base + timedelta(seconds=6)
    assert queued.stderr == "first failure"

    assert (
        store.claim_next(
            "worker-b",
            lease_seconds=60,
            now=base + timedelta(seconds=5),
        )
        is None
    )
    second = store.claim_next(
        "worker-b",
        lease_seconds=60,
        now=base + timedelta(seconds=6),
    )
    assert second is not None
    assert second.execution.attempt == 2

    final = store.finish(
        record.id,
        ExecutionStatus.FAILED,
        exit_code=8,
        stdout="",
        stderr="second failure",
        lease_token=second.lease_token,
        now=base + timedelta(seconds=7),
    )
    assert final.status == ExecutionStatus.FAILED
    assert final.attempt == 2
    assert final.next_attempt_at is None
    assert final.exit_code == 8

    attempts = store.attempts(record.id)
    assert [attempt.status for attempt in attempts] == [
        ExecutionStatus.FAILED,
        ExecutionStatus.FAILED,
    ]
    effects = store.effects(record.id)
    assert [effect.kind for effect in effects].count("retry_scheduled") == 1
    assert effects[-1].kind == "execution_finished"
    store.close()


def test_retry_backoff_is_exponential_and_capped(tmp_path: Path):
    store = ExecutionStore(tmp_path / "db.sqlite3")
    base = utc_now()
    spec = ExecutionSpec(
        argv=["echo", "retry"],
        retry_policy={
            "max_attempts": 4,
            "retry_on": ["FAILED"],
            "backoff_initial_seconds": 3,
            "backoff_multiplier": 4,
            "backoff_max_seconds": 10,
        },
    )
    record, _ = store.create_execution(spec, "retry-backoff")

    expected_delays = [3, 10, 10]
    now = base
    for attempt_number, delay in enumerate(expected_delays, start=1):
        claim = store.claim_next("worker-a", lease_seconds=60, now=now)
        assert claim is not None
        finished_at = now + timedelta(seconds=1)
        queued = store.finish(
            record.id,
            ExecutionStatus.FAILED,
            exit_code=1,
            stdout="",
            stderr=f"attempt {attempt_number}",
            lease_token=claim.lease_token,
            now=finished_at,
        )
        assert queued.status == ExecutionStatus.QUEUED
        assert queued.next_attempt_at == finished_at + timedelta(seconds=delay)
        now = finished_at + timedelta(seconds=delay)

    last = store.claim_next("worker-a", lease_seconds=60, now=now)
    assert last is not None
    final = store.finish(
        record.id,
        ExecutionStatus.SUCCEEDED,
        exit_code=0,
        stdout="ok",
        stderr="",
        lease_token=last.lease_token,
        now=now + timedelta(seconds=1),
    )
    assert final.status == ExecutionStatus.SUCCEEDED
    assert final.attempt == 4
    store.close()


def test_cancelled_backoff_does_not_run_again(tmp_path: Path):
    store = ExecutionStore(tmp_path / "db.sqlite3")
    base = utc_now()
    spec = ExecutionSpec(
        argv=["echo", "retry"],
        retry_policy={
            "max_attempts": 3,
            "retry_on": ["FAILED"],
            "backoff_initial_seconds": 30,
        },
    )
    record, _ = store.create_execution(spec, "cancel-backoff")
    claim = store.claim_next("worker-a", lease_seconds=60, now=base)
    assert claim is not None
    queued = store.finish(
        record.id,
        ExecutionStatus.FAILED,
        exit_code=1,
        stdout="",
        stderr="failure",
        lease_token=claim.lease_token,
        now=base + timedelta(seconds=1),
    )
    assert queued.status == ExecutionStatus.QUEUED
    assert queued.next_attempt_at is not None

    cancelled = store.request_cancel(record.id)
    assert cancelled.status == ExecutionStatus.CANCELLED
    assert cancelled.next_attempt_at is None
    assert store.claim_next(
        "worker-b",
        lease_seconds=60,
        now=base + timedelta(hours=1),
    ) is None
    store.close()


def test_expired_lease_can_schedule_interrupted_retry(tmp_path: Path):
    store = ExecutionStore(tmp_path / "db.sqlite3")
    base = utc_now()
    spec = ExecutionSpec(
        argv=["echo", "retry"],
        retry_policy={
            "max_attempts": 2,
            "retry_on": ["INTERRUPTED"],
            "backoff_initial_seconds": 4,
        },
    )
    record, _ = store.create_execution(spec, "retry-expiry")
    claim = store.claim_next("worker-a", lease_seconds=5, now=base)
    assert claim is not None

    expired_at = base + timedelta(seconds=10)
    assert store.expire_leases(now=expired_at) == [record.id]
    queued = store.get(record.id)
    assert queued.status == ExecutionStatus.QUEUED
    assert queued.next_attempt_at == expired_at + timedelta(seconds=4)
    assert store.attempts(record.id)[0].status == ExecutionStatus.INTERRUPTED
    kinds = [effect.kind for effect in store.effects(record.id)]
    assert kinds[-2:] == ["lease_expired", "retry_scheduled"]

    assert store.claim_next(
        "worker-b",
        lease_seconds=5,
        now=expired_at + timedelta(seconds=3),
    ) is None
    second = store.claim_next(
        "worker-b",
        lease_seconds=5,
        now=expired_at + timedelta(seconds=4),
    )
    assert second is not None
    final = store.finish(
        record.id,
        ExecutionStatus.SUCCEEDED,
        exit_code=0,
        stdout="ok",
        stderr="",
        lease_token=second.lease_token,
        now=expired_at + timedelta(seconds=5),
    )
    assert final.status == ExecutionStatus.SUCCEEDED
    store.close()
