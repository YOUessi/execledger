from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from execledger.models import ExecutionSpec

_INTERNAL_LIMITS_ENV = "EXECLEDGER_INTERNAL_RESOURCE_LIMITS"


class BackendConfigurationError(RuntimeError):
    pass


@dataclass(frozen=True)
class BackendLaunch:
    process: asyncio.subprocess.Process
    backend: str
    applied_limits: dict[str, int]


class ProcessBackend(Protocol):
    name: str
    kernel_resource_limits_supported: bool

    async def launch(
        self,
        spec: ExecutionSpec,
        workspace: Path,
        env: dict[str, str],
    ) -> BackendLaunch: ...

    def resource_exit_reason(
        self,
        returncode: int | None,
        spec: ExecutionSpec,
    ) -> str | None: ...


class LocalProcessBackend:
    name = "local"
    kernel_resource_limits_supported = os.name == "posix"

    @staticmethod
    def _kernel_limits(spec: ExecutionSpec) -> dict[str, int]:
        limits = spec.resource_limits
        configured: dict[str, int] = {}
        if limits.cpu_seconds is not None:
            configured["cpu_seconds"] = limits.cpu_seconds
        if limits.memory_bytes is not None:
            configured["memory_bytes"] = limits.memory_bytes
        if limits.file_size_bytes is not None:
            configured["file_size_bytes"] = limits.file_size_bytes
        if limits.open_files is not None:
            configured["open_files"] = limits.open_files
        return configured

    async def launch(
        self,
        spec: ExecutionSpec,
        workspace: Path,
        env: dict[str, str],
    ) -> BackendLaunch:
        child_env = dict(env)
        child_env.pop(_INTERNAL_LIMITS_ENV, None)
        kernel_limits = self._kernel_limits(spec)

        argv = list(spec.argv)
        if kernel_limits:
            if not self.kernel_resource_limits_supported:
                raise BackendConfigurationError(
                    "kernel resource limits require a POSIX local backend"
                )
            child_env[_INTERNAL_LIMITS_ENV] = json.dumps(
                kernel_limits,
                sort_keys=True,
                separators=(",", ":"),
            )
            argv = [
                sys.executable,
                "-m",
                "execledger.local_exec",
                "--",
                *spec.argv,
            ]

        process = await asyncio.create_subprocess_exec(
            *argv,
            cwd=workspace,
            env=child_env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=os.name == "posix",
        )
        return BackendLaunch(
            process=process,
            backend=self.name,
            applied_limits=kernel_limits,
        )

    def resource_exit_reason(
        self,
        returncode: int | None,
        spec: ExecutionSpec,
    ) -> str | None:
        if returncode is None or returncode >= 0 or os.name != "posix":
            return None
        signum = -returncode

        if (
            spec.resource_limits.cpu_seconds is not None
            and hasattr(signal, "SIGXCPU")
            and signum == int(signal.SIGXCPU)
        ):
            return "cpu_seconds"

        if (
            spec.resource_limits.file_size_bytes is not None
            and hasattr(signal, "SIGXFSZ")
            and signum == int(signal.SIGXFSZ)
        ):
            return "file_size_bytes"

        return None
