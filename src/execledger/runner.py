from __future__ import annotations

import asyncio
import os
import signal
from pathlib import Path

from execledger.models import ExecutionRecord, ExecutionStatus
from execledger.store import ExecutionStore
from execledger.workspace import WorkspaceManager


class ExecutionRunner:
    def __init__(
        self,
        store: ExecutionStore,
        workspaces: WorkspaceManager,
        *,
        workers: int = 2,
        poll_interval: float = 0.05,
    ):
        if workers < 1:
            raise ValueError("workers must be positive")
        self.store = store
        self.workspaces = workspaces
        self.workers = workers
        self.poll_interval = poll_interval
        self._tasks: list[asyncio.Task[None]] = []
        self._processes: dict[str, asyncio.subprocess.Process] = {}
        self._shutdown_ids: set[str] = set()
        self._stopping = asyncio.Event()

    async def start(self) -> None:
        if self._tasks:
            return
        self._stopping.clear()
        self._shutdown_ids.clear()
        self._tasks = [asyncio.create_task(self._worker(i)) for i in range(self.workers)]

    async def stop(self) -> None:
        self._stopping.set()
        for execution_id, process in list(self._processes.items()):
            if process.returncode is None:
                self._shutdown_ids.add(execution_id)
                self.store.add_effect(execution_id, "shutdown_interruption_requested", {})
                self._terminate(process)
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    async def cancel(self, execution_id: str) -> ExecutionRecord:
        record = self.store.request_cancel(execution_id)
        process = self._processes.get(execution_id)
        if process is not None:
            self._terminate(process)
        return record

    async def _worker(self, worker_id: int) -> None:
        while not self._stopping.is_set():
            record = self.store.claim_next()
            if record is None:
                try:
                    await asyncio.wait_for(self._stopping.wait(), timeout=self.poll_interval)
                except TimeoutError:
                    pass
                continue
            if self._stopping.is_set():
                self.store.add_effect(record.id, "shutdown_before_launch", {"worker": worker_id})
                self.store.finish(
                    record.id,
                    ExecutionStatus.INTERRUPTED,
                    exit_code=None,
                    stdout="",
                    stderr="",
                )
                continue
            self.store.add_effect(record.id, "worker_assigned", {"worker": worker_id})
            await self._execute(record)

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

    async def _execute(self, record: ExecutionRecord) -> None:
        execution_id = record.id
        spec = record.spec
        workspace = self.workspaces.directory(execution_id)
        self.workspaces.snapshot(execution_id, "before")
        env = os.environ.copy()
        env.update(spec.env)
        self.store.add_effect(
            execution_id,
            "process_starting",
            {"argv": spec.argv, "cwd": str(Path(execution_id))},
        )
        stdout_buffer = bytearray()
        stderr_buffer = bytearray()
        stdout_state = {"truncated": False}
        stderr_state = {"truncated": False}
        stream_tasks: list[asyncio.Task[None]] = []
        timed_out = False

        try:
            process = await asyncio.create_subprocess_exec(
                *spec.argv,
                cwd=workspace,
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                start_new_session=os.name == "posix",
            )
            self._processes[execution_id] = process
            if process.stdout is None or process.stderr is None:
                raise RuntimeError("subprocess pipes were not created")

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

            if self._stopping.is_set():
                self._shutdown_ids.add(execution_id)
                self.store.add_effect(execution_id, "shutdown_interruption_requested", {})
                self._terminate(process)

            try:
                await asyncio.wait_for(process.wait(), timeout=spec.timeout_seconds)
            except TimeoutError:
                timed_out = True
                await self._terminate_and_wait(process)

            # A job owns its process group. Clean up descendants that outlive the
            # root process so they cannot leak into later executions.
            self._terminate(process)
            streams_complete = await self._finish_streams(execution_id, stream_tasks)

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

            latest = self.store.get(execution_id)
            if latest.cancel_requested:
                status = ExecutionStatus.CANCELLED
            elif execution_id in self._shutdown_ids:
                status = ExecutionStatus.INTERRUPTED
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
            )
        except FileNotFoundError as exc:
            self.workspaces.snapshot(execution_id, "after")
            self.store.finish(
                execution_id,
                ExecutionStatus.FAILED,
                exit_code=None,
                stdout="",
                stderr=str(exc),
            )
        finally:
            for task in stream_tasks:
                if not task.done():
                    task.cancel()
            if stream_tasks:
                await asyncio.gather(*stream_tasks, return_exceptions=True)
            self._processes.pop(execution_id, None)
            self._shutdown_ids.discard(execution_id)
