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
                argv=[sys.executable, "-c", "from pathlib import Path; print(Path('input.txt').read_text()); Path('out.txt').write_text('done')"],
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
            ExecutionSpec(argv=[sys.executable, "-c", "import sys; print('bad', file=sys.stderr); sys.exit(7)"]),
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
            ExecutionSpec(argv=[sys.executable, "-c", "import time; time.sleep(30)"], timeout_seconds=60),
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
