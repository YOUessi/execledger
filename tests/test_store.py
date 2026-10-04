from pathlib import Path

import pytest

from execledger.models import ExecutionSpec, ExecutionStatus
from execledger.store import ExecutionStore, IdempotencyConflict


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


def test_claim_and_finish(tmp_path: Path):
    store = ExecutionStore(tmp_path / "db.sqlite3")
    record, _ = store.create_execution(ExecutionSpec(argv=["echo", "ok"]), "k")
    claimed = store.claim_next()
    assert claimed is not None
    assert claimed.id == record.id
    assert claimed.status == ExecutionStatus.RUNNING
    assert claimed.attempt == 1
    done = store.finish(
        record.id,
        ExecutionStatus.SUCCEEDED,
        exit_code=0,
        stdout="ok\n",
        stderr="",
    )
    assert done.status == ExecutionStatus.SUCCEEDED
    assert done.exit_code == 0
    effects = store.effects(record.id)
    assert [e.kind for e in effects] == [
        "execution_submitted",
        "execution_claimed",
        "execution_finished",
    ]
    store.close()


def test_recovery_marks_running_as_interrupted(tmp_path: Path):
    store = ExecutionStore(tmp_path / "db.sqlite3")
    record, _ = store.create_execution(ExecutionSpec(argv=["echo", "ok"]), "k")
    store.claim_next()
    recovered = store.recover_running()
    assert recovered == [record.id]
    assert store.get(record.id).status == ExecutionStatus.INTERRUPTED
    assert store.effects(record.id)[-1].kind == "recovered_as_interrupted"
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
