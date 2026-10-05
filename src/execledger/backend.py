from __future__ import annotations

import asyncio
import json
import os
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path

from execledger.models import ExecutionSpec, ResourceUsage


@dataclass(frozen=True)
class SpawnedProcess:
    process: asyncio.subprocess.Process
    result_path: Path


@dataclass(frozen=True)
class BackendResult:
    exit_code: int | None
    resource_usage: ResourceUsage | None


class SubprocessBackend:
    name = "subprocess"

    def __init__(self, runtime_root: Path):
        self.runtime_root = runtime_root.resolve()
        self.runtime_root.mkdir(mode=0o700, parents=True, exist_ok=True)

    @property
    def supports_resource_limits(self) -> bool:
        return os.name == "posix"

    async def spawn(
        self,
        execution_id: str,
        attempt_number: int,
        spec: ExecutionSpec,
        *,
        cwd: Path,
        env: dict[str, str],
    ) -> SpawnedProcess:
        if spec.resource_limits.configured() and not self.supports_resource_limits:
            raise RuntimeError("resource limits require a POSIX execution backend")

        result_dir = self.runtime_root / execution_id
        result_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        result_path = result_dir / f"attempt-{attempt_number}-{uuid.uuid4().hex}.json"

        launch_env = dict(env)
        launch_env["EXECLEDGER_RESOURCE_LIMITS"] = json.dumps(
            spec.resource_limits.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
        )

        if os.name == "posix":
            argv = [
                sys.executable,
                "-m",
                "execledger.launcher",
                str(result_path),
                "--",
                *spec.argv,
            ]
        else:
            argv = spec.argv

        process = await asyncio.create_subprocess_exec(
            *argv,
            cwd=cwd,
            env=launch_env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=os.name == "posix",
        )
        return SpawnedProcess(process=process, result_path=result_path)

    def collect_result(self, spawned: SpawnedProcess) -> BackendResult:
        if not spawned.result_path.exists():
            return BackendResult(
                exit_code=spawned.process.returncode,
                resource_usage=None,
            )
        try:
            payload = json.loads(spawned.result_path.read_text(encoding="utf-8"))
            usage = ResourceUsage.model_validate(payload["usage"])
            exit_code = int(payload["target_returncode"])
            return BackendResult(exit_code=exit_code, resource_usage=usage)
        except (OSError, ValueError, KeyError, TypeError):
            return BackendResult(
                exit_code=spawned.process.returncode,
                resource_usage=None,
            )
        finally:
            try:
                spawned.result_path.unlink()
                spawned.result_path.parent.rmdir()
            except OSError:
                pass
