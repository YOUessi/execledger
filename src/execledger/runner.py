from __future__ import annotations

import asyncio
import os
import signal
import socket
import time
import uuid
from pathlib import Path

from execledger.backends import LocalProcessBackend, ProcessBackend
from execledger.models import ExecutionRecord, ExecutionStatus
from execledger.store import ClaimedExecution, ExecutionStore, LostLease
from execledger.workspace import WorkspaceManager


class ExecutionRunner:
    def __init__(
        self,
        store: ExecutionStore,
        workspaces: WorkspaceManager,
        *,
        workers: int = 2,
        poll_interval: float = 0.05,
        lease_seconds: float = 5.0,
        heartbeat_interval: float | None = None,
        worker_id: str | None = None,
        backend: ProcessBackend | None = None,
        resource_poll_interval: float = 0.1,
    ):
        if workers < 1:
            raise ValueError("workers must be positive")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        if resource_poll_interval <= 0:
            raise ValueError("resource_poll_interval must be positive")
        heartbeat = heartbeat_interval or min(1.0, lease_seconds / 3)
        if heartbeat <= 0 or heartbeat >= lease_seconds:
            raise ValueError("heartbeat_interval must be positive and less than lease_seconds")

        self.store = store
        self.workspaces = workspaces
        self.workers = workers
        self.poll_interval = poll_interval
        self.lease_seconds = lease_seconds
        self.heartbeat_interval = heartbeat
        self.worker_id = worker_id or (
            f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        )
        self.backend = backend or LocalProcessBackend()
        self.resource_poll_interval = resource_poll_interval
        self._tasks: list[asyncio.Task[None]] = []
        self._processes: dict[str, asyncio.subprocess.Process] = {}
        self._shutdown_ids: set[str] = set()
        self._stopping = asyncio.Event()
        self._last_expiry_sweep = 0.0
        self._expiry_sweep_interval = min(1.0, max(0.05, lease_seconds / 2))

    async def start(self) -> None:
        if self._tasks:
            return
        self._stopping.clear()
        self._shutdown_ids.clear()
        self._sweep_expired(force=True)
        self._tasks = [asyncio.create_task(self._worker(i)) for i in range(self.workers)]

    async def stop(self) -> None:
        self._stopping.set()
        stopping: list[asyncio.subprocess.Process] = []
        for execution_id, process in list(self._processes.items()):
            if process.returncode is None:
                self._shutdown_ids.add(execution_id)
                self.store.add_effect(execution_id, "shutdown_interruption_requested", {})
                stopping.append(process)
        if stopping:
            await asyncio.gather(
                *(self._terminate_and_wait(process) for process in stopping),
                return_exceptions=True,
            )
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    def diagnostics(self) -> dict[str, object]:
        return {
            "worker_id": self.worker_id,
            "workers_configured": self.workers,
            "active_processes": sum(
                1 for process in self._processes.values() if process.returncode is None
            ),
            "stopping": self._stopping.is_set(),
            "lease_seconds": self.lease_seconds,
            "heartbeat_interval": self.heartbeat_interval,
            "backend": self.backend.name,
            "kernel_resource_limits_supported": (
                self.backend.kernel_resource_limits_supported
            ),
        }

    async def cancel(self, execution_id: str) -> ExecutionRecord:
        record = self.store.request_cancel(execution_id)
        process = self._processes.get(execution_id)
        if process is not None and process.returncode is None:
            await self._terminate_and_wait(process)
        return record

    def _sweep_expired(self, *, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self._last_expiry_sweep < self._expiry_sweep_interval:
            return
        self._last_expiry_sweep = now
        self.store.expire_leases()

    async def _worker(self, worker_slot: int) -> None:
        worker_identity = f"{self.worker_id}/{worker_slot}"
        while not self._stopping.is_set():
            self._sweep_expired()
            claim = self.store.claim_next(
                worker_identity,
                lease_seconds=self.lease_seconds,
            )
            if claim is None:
                try:
                    await asyncio.wait_for(self._stopping.wait(), timeout=self.poll_interval)
                except TimeoutError:
                    pass
                continue

            record = claim.execution
            if self._stopping.is_set():
                self.store.add_effect(
                    record.id,
                    "shutdown_before_launch",
                    {"worker_id": worker_identity},
                )
                try:
                    self.store.finish(
                        record.id,
                        ExecutionStatus.INTERRUPTED,
                        exit_code=None,
                        stdout="",
                        stderr="",
                        lease_token=claim.lease_token,
                    )
                except LostLease:
                    pass
                continue

            self.store.add_effect(
                record.id,
                "worker_assigned",
                {
                    "worker_id": worker_identity,
                    "attempt": record.attempt,
                },
            )
            await self._execute(claim)

    @staticmethod
    def _terminate(process: asyncio.subprocess.Process) -> None:
        if os.name == "posix":
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            return
        if process.returncode is None:
            process.terminate()

    @staticmethod
    def _kill(process: asyncio.subprocess.Process) -> None:
        if os.name == "posix":
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            return
        if process.returncode is None:
            process.kill()

    async def _terminate_and_wait(self, process: asyncio.subprocess.Process) -> None:
        self._terminate(process)
        if process.returncode is not None:
            return
        try:
            await asyncio.wait_for(process.wait(), timeout=2)
        except TimeoutError:
            self._kill(process)
            await process.wait()

    async def _heartbeat(
        self,
        execution_id: str,
        lease_token: str,
        process: asyncio.subprocess.Process,
        lease_lost: asyncio.Event,
    ) -> None:
        while process.returncode is None and not self._stopping.is_set():
            await asyncio.sleep(self.heartbeat_interval)
            if process.returncode is not None:
                return

            record = self.store.get(execution_id)
            if record.cancel_requested:
                await self._terminate_and_wait(process)
                return

            renewed = self.store.renew_lease(
                execution_id,
                lease_token,
                lease_seconds=self.lease_seconds,
            )
            if not renewed:
                lease_lost.set()
                await self._terminate_and_wait(process)
                return

    async def _monitor_workspace_limit(
        self,
        execution_id: str,
        process: asyncio.subprocess.Process,
        limit: int,
        exceeded: asyncio.Event,
    ) -> None:
        while process.returncode is None and not self._stopping.is_set():
            usage = self.workspaces.usage_bytes(execution_id)
            if usage > limit:
                exceeded.set()
                self.store.add_effect(
                    execution_id,
                    "resource_limit_exceeded",
                    {
                        "resource": "workspace_bytes",
                        "limit": limit,
                        "observed": usage,
                    },
                )
                await self._terminate_and_wait(process)
                return
            await asyncio.sleep(self.resource_poll_interval)

    async def _drain_stream(
        self,
        execution_id: str,
        stream: str,
        reader: asyncio.StreamReader,
        limit: int,
        captured: bytearray,
        state: dict[str, bool],
    ) -> None:
        while True:
            chunk = await reader.read(8192)
            if not chunk:
                return
            remaining = max(0, limit - len(captured))
            accepted = chunk[:remaining]
            if accepted:
                captured.extend(accepted)
                self.store.add_effect(
                    execution_id,
                    "output_chunk",
                    {
                        "stream": stream,
                        "data": accepted.decode("utf-8", errors="replace"),
                        "bytes": len(accepted),
                    },
                )
            if len(accepted) < len(chunk):
                state["truncated"] = True

    async def _finish_streams(
        self,
        execution_id: str,
        tasks: list[asyncio.Task[None]],
    ) -> bool:
        if not tasks:
            return True
        try:
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=2)
            return True
        except TimeoutError:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self.store.add_effect(execution_id, "output_drain_incomplete", {})
            return False

    async def _execute(self, claim: ClaimedExecution) -> None:
        record = claim.execution
        execution_id = record.id
        lease_token = claim.lease_token
        spec = record.spec
        workspace = self.workspaces.directory(execution_id)

        stdout_buffer = bytearray()
        stderr_buffer = bytearray()
        stdout_state = {"truncated": False}
        stderr_state = {"truncated": False}
        stream_tasks: list[asyncio.Task[None]] = []
        heartbeat_task: asyncio.Task[None] | None = None
        workspace_task: asyncio.Task[None] | None = None
        lease_lost = asyncio.Event()
        workspace_exceeded = asyncio.Event()
        timed_out = False
        process: asyncio.subprocess.Process | None = None

        try:
            if not self.store.renew_lease(
                execution_id,
                lease_token,
                lease_seconds=self.lease_seconds,
            ):
                return
            self.workspaces.snapshot(execution_id, "before")

            workspace_limit = spec.resource_limits.workspace_bytes
            if workspace_limit is not None:
                initial_usage = self.workspaces.usage_bytes(execution_id)
                if initial_usage > workspace_limit:
                    self.store.add_effect(
                        execution_id,
                        "resource_limit_exceeded",
                        {
                            "resource": "workspace_bytes",
                            "limit": workspace_limit,
                            "observed": initial_usage,
                            "phase": "before_launch",
                        },
                    )
                    self.workspaces.snapshot(execution_id, "after")
                    self.store.finish(
                        execution_id,
                        ExecutionStatus.RESOURCE_EXHAUSTED,
                        exit_code=None,
                        stdout="",
                        stderr=(
                            "workspace limit exceeded before process launch: "
                            f"{initial_usage} > {workspace_limit}"
                        ),
                        lease_token=lease_token,
                    )
                    return

            if not self.store.renew_lease(
                execution_id,
                lease_token,
                lease_seconds=self.lease_seconds,
            ):
                return
            env = os.environ.copy()
            env.update(spec.env)
            configured_limits = spec.resource_limits.configured()
            if configured_limits:
                self.store.add_effect(
                    execution_id,
                    "resource_limits_configured",
                    configured_limits,
                )
            self.store.add_effect(
                execution_id,
                "process_starting",
                {
                    "argv": spec.argv,
                    "cwd": str(Path(execution_id)),
                    "backend": self.backend.name,
                },
            )

            launched = await self.backend.launch(spec, workspace, env)
            process = launched.process
            self._processes[execution_id] = process
            if process.stdout is None or process.stderr is None:
                raise RuntimeError("subprocess pipes were not created")

            if not self.store.renew_lease(
                execution_id,
                lease_token,
                lease_seconds=self.lease_seconds,
            ):
                lease_lost.set()
                self.store.add_effect(
                    execution_id,
                    "lease_lost_after_spawn",
                    {},
                )
                await self._terminate_and_wait(process)
                return

            stream_tasks = [
                asyncio.create_task(
                    self._drain_stream(
                        execution_id,
                        "stdout",
                        process.stdout,
                        spec.max_output_bytes,
                        stdout_buffer,
                        stdout_state,
                    )
                ),
                asyncio.create_task(
                    self._drain_stream(
                        execution_id,
                        "stderr",
                        process.stderr,
                        spec.max_output_bytes,
                        stderr_buffer,
                        stderr_state,
                    )
                ),
            ]
            heartbeat_task = asyncio.create_task(
                self._heartbeat(
                    execution_id,
                    lease_token,
                    process,
                    lease_lost,
                )
            )
            if workspace_limit is not None:
                workspace_task = asyncio.create_task(
                    self._monitor_workspace_limit(
                        execution_id,
                        process,
                        workspace_limit,
                        workspace_exceeded,
                    )
                )

            if self._stopping.is_set():
                self._shutdown_ids.add(execution_id)
                self.store.add_effect(execution_id, "shutdown_interruption_requested", {})
                self._terminate(process)

            try:
                await asyncio.wait_for(process.wait(), timeout=spec.timeout_seconds)
            except TimeoutError:
                timed_out = True
                await self._terminate_and_wait(process)

            self._terminate(process)
            streams_complete = await self._finish_streams(execution_id, stream_tasks)

            if heartbeat_task is not None:
                heartbeat_task.cancel()
                await asyncio.gather(heartbeat_task, return_exceptions=True)
            if workspace_task is not None:
                workspace_task.cancel()
                await asyncio.gather(workspace_task, return_exceptions=True)

            # Revalidate ownership immediately before publishing final evidence/state.
            if lease_lost.is_set() or not self.store.renew_lease(
                execution_id,
                lease_token,
                lease_seconds=self.lease_seconds,
            ):
                return

            stdout = stdout_buffer.decode("utf-8", errors="replace")
            stderr = stderr_buffer.decode("utf-8", errors="replace")
            self.store.add_effect(
                execution_id,
                "output_captured",
                {
                    "stdout_truncated": stdout_state["truncated"],
                    "stderr_truncated": stderr_state["truncated"],
                    "drain_complete": streams_complete,
                },
            )
            self.workspaces.snapshot(execution_id, "after")

            resource_reason = None
            if workspace_exceeded.is_set():
                resource_reason = "workspace_bytes"
            else:
                resource_reason = self.backend.resource_exit_reason(
                    process.returncode,
                    spec,
                )
                if resource_reason is not None:
                    self.store.add_effect(
                        execution_id,
                        "resource_limit_exceeded",
                        {
                            "resource": resource_reason,
                            "returncode": process.returncode,
                        },
                    )

            latest = self.store.get(execution_id)
            if latest.cancel_requested:
                status = ExecutionStatus.CANCELLED
            elif execution_id in self._shutdown_ids:
                status = ExecutionStatus.INTERRUPTED
            elif resource_reason is not None:
                status = ExecutionStatus.RESOURCE_EXHAUSTED
            elif timed_out:
                status = ExecutionStatus.TIMED_OUT
            else:
                status = (
                    ExecutionStatus.SUCCEEDED if process.returncode == 0 else ExecutionStatus.FAILED
                )
            self.store.finish(
                execution_id,
                status,
                exit_code=process.returncode,
                stdout=stdout,
                stderr=stderr,
                lease_token=lease_token,
            )
        except LostLease:
            if process is not None:
                self._terminate(process)
        except (OSError, RuntimeError) as exc:
            try:
                self.workspaces.snapshot(execution_id, "after")
                self.store.finish(
                    execution_id,
                    ExecutionStatus.FAILED,
                    exit_code=None,
                    stdout="",
                    stderr=str(exc),
                    lease_token=lease_token,
                )
            except LostLease:
                if process is not None:
                    self._terminate(process)
        finally:
            if heartbeat_task is not None and not heartbeat_task.done():
                heartbeat_task.cancel()
                await asyncio.gather(heartbeat_task, return_exceptions=True)
            if workspace_task is not None and not workspace_task.done():
                workspace_task.cancel()
                await asyncio.gather(workspace_task, return_exceptions=True)
            for task in stream_tasks:
                if not task.done():
                    task.cancel()
            if stream_tasks:
                await asyncio.gather(*stream_tasks, return_exceptions=True)
            self._processes.pop(execution_id, None)
            self._shutdown_ids.discard(execution_id)
