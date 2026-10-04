import asyncio
import sys
from pathlib import Path

import pytest

from execledger.models import ExecutionSpec, ExecutionStatus
from execledger.service import ExecutionService


@pytest.mark.asyncio
async def test_successful_execution_and_snapshots(tmp_path: Path):
    service = ExecutionService(tmp_path, workers=1)
    await service.start()
    try:
        result = await service.submit(
            ExecutionSpec(
                argv=[
                    sys.executable,
                    "-c",
                    (
                        "from pathlib import Path; "
                        "print(Path('input.txt').read_text()); "
                        "Path('out.txt').write_text('done')"
                    ),
                ],
                files={"input.txt": "hello"},
            ),
            "success-key",
        )
        final = await service.wait_terminal(result.execution.id)
        assert final.status == ExecutionStatus.SUCCEEDED
        assert final.stdout.strip() == "hello"
        effects = service.store.effects(final.id)
        assert any(
            effect.kind == "output_chunk"
            and effect.payload.get("stream") == "stdout"
            and "hello" in str(effect.payload.get("data"))
            for effect in effects
        )
        snapshots = service.store.snapshots(final.id)
        assert [s.phase for s in snapshots] == ["before", "after"]
        assert any(item["path"] == "out.txt" for item in snapshots[-1].manifest)
    finally:
        await service.stop()
        service.store.close()


@pytest.mark.asyncio
async def test_failed_execution_is_recorded(tmp_path: Path):
    service = ExecutionService(tmp_path, workers=1)
    await service.start()
    try:
        result = await service.submit(
            ExecutionSpec(
                argv=[
                    sys.executable,
                    "-c",
                    "import sys; print('bad', file=sys.stderr); sys.exit(7)",
                ]
            ),
            "fail-key",
        )
        final = await service.wait_terminal(result.execution.id)
        assert final.status == ExecutionStatus.FAILED
        assert final.exit_code == 7
        assert "bad" in final.stderr
    finally:
        await service.stop()
        service.store.close()


@pytest.mark.asyncio
async def test_timeout_is_enforced(tmp_path: Path):
    service = ExecutionService(tmp_path, workers=1)
    await service.start()
    try:
        result = await service.submit(
            ExecutionSpec(
                argv=[sys.executable, "-c", "import time; time.sleep(5)"],
                timeout_seconds=0.1,
            ),
            "timeout-key",
        )
        final = await service.wait_terminal(result.execution.id)
        assert final.status == ExecutionStatus.TIMED_OUT
    finally:
        await service.stop()
        service.store.close()


@pytest.mark.asyncio
async def test_running_execution_can_be_cancelled(tmp_path: Path):
    service = ExecutionService(tmp_path, workers=1)
    await service.start()
    try:
        result = await service.submit(
            ExecutionSpec(
                argv=[sys.executable, "-c", "import time; time.sleep(30)"],
                timeout_seconds=60,
            ),
            "cancel-key",
        )
        execution_id = result.execution.id
        for _ in range(200):
            if service.store.get(execution_id).status == ExecutionStatus.RUNNING:
                break
            await asyncio.sleep(0.01)
        await service.cancel(execution_id)
        final = await service.wait_terminal(execution_id)
        assert final.status == ExecutionStatus.CANCELLED
        assert service.store.get(execution_id).cancel_requested is True
    finally:
        await service.stop()
        service.store.close()


@pytest.mark.asyncio
async def test_output_is_bounded(tmp_path: Path):
    service = ExecutionService(tmp_path, workers=1)
    await service.start()
    try:
        result = await service.submit(
            ExecutionSpec(
                argv=[sys.executable, "-c", "print('x' * 5000)"],
                max_output_bytes=1024,
            ),
            "bound-key",
        )
        final = await service.wait_terminal(result.execution.id)
        assert final.status == ExecutionStatus.SUCCEEDED
        assert len(final.stdout.encode()) <= 1024
        effects = service.store.effects(final.id)
        capture = next(e for e in effects if e.kind == "output_captured")
        assert capture.payload["stdout_truncated"] is True
    finally:
        await service.stop()
        service.store.close()


@pytest.mark.asyncio
async def test_graceful_shutdown_marks_running_execution_interrupted(tmp_path: Path):
    service = ExecutionService(tmp_path, workers=1)
    await service.start()
    result = await service.submit(
        ExecutionSpec(
            argv=[sys.executable, "-c", "import time; time.sleep(30)"],
            timeout_seconds=60,
        ),
        "shutdown-key",
    )
    execution_id = result.execution.id
    for _ in range(200):
        if service.store.get(execution_id).status == ExecutionStatus.RUNNING:
            break
        await asyncio.sleep(0.01)

    await service.stop()

    final = service.store.get(execution_id)
    assert final.status == ExecutionStatus.INTERRUPTED
    assert any(
        effect.kind == "shutdown_interruption_requested"
        for effect in service.store.effects(execution_id)
    )
    service.store.close()


@pytest.mark.asyncio
@pytest.mark.skipif(sys.platform == "win32", reason="process-group semantics are POSIX-specific")
async def test_cancel_terminates_child_process_group(tmp_path: Path):
    service = ExecutionService(tmp_path, workers=1)
    await service.start()
    try:
        child = (
            "import time; "
            "from pathlib import Path; "
            "time.sleep(0.5); "
            "Path('child-survived.txt').write_text('unexpected')"
        )
        parent = (
            "import subprocess, sys, time; "
            "from pathlib import Path; "
            f"subprocess.Popen([sys.executable, '-c', {child!r}]); "
            "Path('child-spawned.txt').write_text('ready'); "
            "time.sleep(30)"
        )
        result = await service.submit(
            ExecutionSpec(
                argv=[sys.executable, "-c", parent],
                timeout_seconds=60,
            ),
            "process-tree-key",
        )
        execution_id = result.execution.id
        workspace = service.workspaces.directory(execution_id)
        for _ in range(300):
            if (workspace / "child-spawned.txt").exists():
                break
            await asyncio.sleep(0.01)
        assert (workspace / "child-spawned.txt").exists()

        await service.cancel(execution_id)
        final = await service.wait_terminal(execution_id)
        assert final.status == ExecutionStatus.CANCELLED

        await asyncio.sleep(0.8)
        assert not (workspace / "child-survived.txt").exists()
    finally:
        await service.stop()
        service.store.close()


@pytest.mark.asyncio
async def test_missing_executable_fails_without_crashing_worker(tmp_path: Path):
    service = ExecutionService(tmp_path, workers=1)
    await service.start()
    try:
        result = await service.submit(
            ExecutionSpec(argv=["definitely-not-an-execledger-command"]),
            "missing-command-key",
        )
        final = await service.wait_terminal(result.execution.id)
        assert final.status == ExecutionStatus.FAILED
        assert final.exit_code is None
        assert final.stderr
    finally:
        await service.stop()
        service.store.close()


@pytest.mark.asyncio
async def test_two_services_share_one_sqlite_queue_without_double_claim(tmp_path: Path):
    service_a = ExecutionService(
        tmp_path,
        workers=1,
        lease_seconds=0.5,
        heartbeat_interval=0.1,
        worker_id="service-a",
    )
    service_b = ExecutionService(
        tmp_path,
        workers=1,
        lease_seconds=0.5,
        heartbeat_interval=0.1,
        worker_id="service-b",
    )
    result = await service_a.submit(
        ExecutionSpec(
            argv=[
                sys.executable,
                "-c",
                "from pathlib import Path; Path('owner.txt').write_text('once'); print('done')",
            ]
        ),
        "shared-queue-key",
    )
    await asyncio.gather(service_a.start(), service_b.start())
    try:
        final = await service_a.wait_terminal(result.execution.id)
        assert final.status == ExecutionStatus.SUCCEEDED
        attempts = service_a.store.attempts(final.id)
        assert len(attempts) == 1
        assert attempts[0].worker_id in {"service-a/0", "service-b/0"}
        assert (service_a.workspaces.directory(final.id) / "owner.txt").read_text() == "once"
    finally:
        await asyncio.gather(service_a.stop(), service_b.stop())
        service_a.store.close()
        service_b.store.close()


@pytest.mark.asyncio
async def test_cancel_from_non_owner_service_is_observed_by_lease_heartbeat(tmp_path: Path):
    service_a = ExecutionService(
        tmp_path,
        workers=1,
        lease_seconds=0.5,
        heartbeat_interval=0.05,
        worker_id="service-a",
    )
    service_b = ExecutionService(
        tmp_path,
        workers=1,
        lease_seconds=0.5,
        heartbeat_interval=0.05,
        worker_id="service-b",
    )
    result = await service_a.submit(
        ExecutionSpec(
            argv=[sys.executable, "-c", "import time; time.sleep(30)"],
            timeout_seconds=60,
        ),
        "remote-cancel-key",
    )
    await asyncio.gather(service_a.start(), service_b.start())
    try:
        execution_id = result.execution.id
        for _ in range(300):
            if service_a.store.get(execution_id).status == ExecutionStatus.RUNNING:
                break
            await asyncio.sleep(0.01)
        assert service_a.store.get(execution_id).status == ExecutionStatus.RUNNING

        owner = service_a.store.get(execution_id).worker_id
        non_owner = service_b if owner and owner.startswith("service-a") else service_a
        await non_owner.cancel(execution_id)

        final = await service_a.wait_terminal(execution_id, timeout=5)
        assert final.status == ExecutionStatus.CANCELLED
        assert final.cancel_requested is True
    finally:
        await asyncio.gather(service_a.stop(), service_b.stop())
        service_a.store.close()
        service_b.store.close()
