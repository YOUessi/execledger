import os
import sys
from pathlib import Path

import pytest

from execledger.models import ExecutionSpec, ExecutionStatus
from execledger.service import ExecutionService


@pytest.mark.asyncio
async def test_subprocess_backend_records_resource_usage(tmp_path: Path):
    service = ExecutionService(
        tmp_path,
        workers=1,
        worker_id="usage-test",
    )
    await service.start()
    try:
        result = await service.submit(
            ExecutionSpec(
                argv=[
                    sys.executable,
                    "-c",
                    "sum(i * i for i in range(200000)); print('ok')",
                ]
            ),
            "usage-key",
        )
        final = await service.wait_terminal(result.execution.id)
        assert final.status == ExecutionStatus.SUCCEEDED
        assert final.stdout.strip() == "ok"
        assert final.resource_usage is not None
        assert final.resource_usage.wall_time_seconds >= 0
        assert final.resource_usage.user_cpu_seconds >= 0
        assert final.resource_usage.system_cpu_seconds >= 0
        assert final.resource_usage.max_rss_bytes > 0

        attempts = service.store.attempts(final.id)
        assert len(attempts) == 1
        assert attempts[0].resource_usage is not None
        assert (
            attempts[0].resource_usage.max_rss_bytes
            == final.resource_usage.max_rss_bytes
        )
    finally:
        await service.stop()
        service.store.close()


@pytest.mark.asyncio
@pytest.mark.skipif(os.name != "posix", reason="POSIX RLIMIT test")
async def test_file_size_limit_is_enforced_without_server_preexec(tmp_path: Path):
    service = ExecutionService(
        tmp_path,
        workers=1,
        worker_id="limit-test",
    )
    await service.start()
    try:
        result = await service.submit(
            ExecutionSpec(
                argv=[
                    sys.executable,
                    "-c",
                    (
                        "from pathlib import Path; "
                        "Path('too-large.bin').write_bytes(b'x' * 65536)"
                    ),
                ],
                resource_limits={"max_file_bytes": 1024},
            ),
            "file-limit-key",
        )
        final = await service.wait_terminal(result.execution.id)
        assert final.status == ExecutionStatus.FAILED
        assert final.exit_code not in {0}
        assert final.resource_usage is not None

        target = service.workspaces.directory(final.id) / "too-large.bin"
        if target.exists():
            assert target.stat().st_size <= 1024
    finally:
        await service.stop()
        service.store.close()


@pytest.mark.asyncio
async def test_missing_executable_keeps_none_exit_code_with_backend(tmp_path: Path):
    service = ExecutionService(tmp_path, workers=1)
    await service.start()
    try:
        result = await service.submit(
            ExecutionSpec(argv=["definitely-not-an-execledger-command"]),
            "backend-missing-command",
        )
        final = await service.wait_terminal(result.execution.id)
        assert final.status == ExecutionStatus.FAILED
        assert final.exit_code is None
        assert "execledger launcher" in final.stderr
        assert final.resource_usage is not None
    finally:
        await service.stop()
        service.store.close()
