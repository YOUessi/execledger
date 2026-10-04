from __future__ import annotations

import asyncio
import os
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
        self._stopping = asyncio.Event()

    async def start(self) -> None:
        if self._tasks:
            return
        self._stopping.clear()
        self._tasks = [asyncio.create_task(self._worker(i)) for i in range(self.workers)]

    async def stop(self) -> None:
        self._stopping.set()
        for process in list(self._processes.values()):
            if process.returncode is None:
                process.terminate()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    async def cancel(self, execution_id: str) -> ExecutionRecord:
        record = self.store.request_cancel(execution_id)
        process = self._processes.get(execution_id)
        if process is not None and process.returncode is None:
            process.terminate()
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
            self.store.add_effect(record.id, "worker_assigned", {"worker": worker_id})
            await self._execute(record)

    @staticmethod
    def _limit(raw: bytes, limit: int) -> tuple[str, bool]:
        truncated = len(raw) > limit
        data = raw[:limit]
        return data.decode("utf-8", errors="replace"), truncated

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
        try:
            process = await asyncio.create_subprocess_exec(
                *spec.argv,
                cwd=workspace,
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            self._processes[execution_id] = process
            try:
                stdout_b, stderr_b = await asyncio.wait_for(
                    process.communicate(), timeout=spec.timeout_seconds
                )
            except TimeoutError:
                process.terminate()
                try:
                    stdout_b, stderr_b = await asyncio.wait_for(process.communicate(), timeout=2)
                except TimeoutError:
                    process.kill()
                    stdout_b, stderr_b = await process.communicate()
                stdout, stdout_truncated = self._limit(stdout_b, spec.max_output_bytes)
                stderr, stderr_truncated = self._limit(stderr_b, spec.max_output_bytes)
                self.store.add_effect(
                    execution_id,
                    "output_captured",
                    {"stdout_truncated": stdout_truncated, "stderr_truncated": stderr_truncated},
                )
                self.workspaces.snapshot(execution_id, "after")
                self.store.finish(
                    execution_id,
                    ExecutionStatus.TIMED_OUT,
                    exit_code=process.returncode,
                    stdout=stdout,
                    stderr=stderr,
                )
                return

            stdout, stdout_truncated = self._limit(stdout_b, spec.max_output_bytes)
            stderr, stderr_truncated = self._limit(stderr_b, spec.max_output_bytes)
            self.store.add_effect(
                execution_id,
                "output_captured",
                {"stdout_truncated": stdout_truncated, "stderr_truncated": stderr_truncated},
            )
            self.workspaces.snapshot(execution_id, "after")
            latest = self.store.get(execution_id)
            if latest.cancel_requested:
                status = ExecutionStatus.CANCELLED
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
            self._processes.pop(execution_id, None)
