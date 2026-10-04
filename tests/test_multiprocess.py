from __future__ import annotations

import multiprocessing as mp
from pathlib import Path

from execledger.models import ExecutionSpec
from execledger.store import ExecutionStore


def _claim_once(
    db_path: str,
    worker_id: str,
    start: mp.synchronize.Event,
    output: mp.queues.Queue,
) -> None:
    store = ExecutionStore(Path(db_path))
    try:
        start.wait(timeout=5)
        claim = store.claim_next(worker_id, lease_seconds=5)
        output.put(claim.execution.id if claim is not None else None)
    finally:
        store.close()


def test_two_processes_do_not_double_claim_one_execution(tmp_path: Path):
    db = tmp_path / "execledger.sqlite3"
    seed = ExecutionStore(db)
    record, _ = seed.create_execution(
        ExecutionSpec(argv=["echo", "ok"]),
        "multiprocess-claim",
    )
    seed.close()

    context = mp.get_context("spawn")
    start = context.Event()
    output = context.Queue()
    processes = [
        context.Process(
            target=_claim_once,
            args=(str(db), worker_id, start, output),
        )
        for worker_id in ("process-a", "process-b")
    ]

    for process in processes:
        process.start()
    start.set()
    for process in processes:
        process.join(timeout=10)
        assert process.exitcode == 0

    results = [output.get(timeout=2), output.get(timeout=2)]
    assert results.count(record.id) == 1
    assert results.count(None) == 1

    check = ExecutionStore(db)
    attempts = check.attempts(record.id)
    assert len(attempts) == 1
    assert attempts[0].worker_id in {"process-a", "process-b"}
    check.close()
