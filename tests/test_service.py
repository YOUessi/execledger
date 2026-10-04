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
