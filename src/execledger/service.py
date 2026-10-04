from __future__ import annotations

import asyncio
from pathlib import Path

from execledger.models import TERMINAL_STATUSES, ExecutionRecord, ExecutionSpec, SubmitResult
from execledger.runner import ExecutionRunner
from execledger.store import ExecutionStore
from execledger.workspace import WorkspaceManager


class ExecutionService:
    def __init__(self, root: Path, *, workers: int = 2):
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.store = ExecutionStore(self.root / "execledger.sqlite3")
        self.workspaces = WorkspaceManager(self.root / "workspaces", self.store)
        self.runner = ExecutionRunner(self.store, self.workspaces, workers=workers)
        self._started = False

    async def start(self) -> None:
        if self._started:
            return
        self.store.recover_running()
        await self.runner.start()
        self._started = True

    async def stop(self) -> None:
        if not self._started:
            return
        await self.runner.stop()
        self._started = False

    async def submit(self, spec: ExecutionSpec, idempotency_key: str) -> SubmitResult:
        record, created = self.store.create_execution(spec, idempotency_key)
        if created:
            try:
                self.workspaces.prepare(record.id, spec)
            except Exception:
                self.store.request_cancel(record.id)
                raise
        return SubmitResult(execution=self.store.get(record.id), created=created)

    async def cancel(self, execution_id: str) -> ExecutionRecord:
        return await self.runner.cancel(execution_id)

    async def wait_terminal(self, execution_id: str, timeout: float = 10.0) -> ExecutionRecord:
        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            record = self.store.get(execution_id)
            if record.status in TERMINAL_STATUSES:
                return record
            if asyncio.get_running_loop().time() >= deadline:
                raise TimeoutError(execution_id)
            await asyncio.sleep(0.02)
