from __future__ import annotations

import asyncio
from pathlib import Path

from execledger.maintenance import StorageMaintenance
from execledger.models import (
    TERMINAL_STATUSES,
    DiagnosticsReport,
    ExecutionRecord,
    ExecutionSpec,
    ReadinessReport,
    SubmitResult,
    utc_now,
)
from execledger.runner import ExecutionRunner
from execledger.store import ExecutionStore
from execledger.workspace import WorkspaceManager


class ExecutionService:
    def __init__(
        self,
        root: Path,
        *,
        workers: int = 2,
        lease_seconds: float = 5.0,
        heartbeat_interval: float | None = None,
        worker_id: str | None = None,
    ):
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.store = ExecutionStore(self.root / "execledger.sqlite3")
        self.workspaces = WorkspaceManager(self.root / "workspaces", self.store)
        self.maintenance = StorageMaintenance(
            self.store,
            self.workspaces.blobs,
            self.workspaces.restore_root,
            self.workspaces.root,
            self.workspaces.maintenance_lock_path,
        )
        self.runner = ExecutionRunner(
            self.store,
            self.workspaces,
            workers=workers,
            lease_seconds=lease_seconds,
            heartbeat_interval=heartbeat_interval,
            worker_id=worker_id,
        )
        self._started = False

    @staticmethod
    def _directory_stats(path: Path) -> tuple[int, int]:
        if not path.exists():
            return 0, 0
        count = 0
        total = 0
        for entry in path.rglob("*"):
            if entry.is_symlink():
                continue
            if entry.is_file():
                count += 1
                total += entry.stat().st_size
        return count, total

    def readiness(self) -> ReadinessReport:
        checks = {
            "database": self.store.ping(),
            "state_root": self.root.exists() and self.root.is_dir(),
            "workspace_root": (
                self.workspaces.root.exists() and self.workspaces.root.is_dir()
            ),
            "blob_root": (
                self.workspaces.blobs.root.exists()
                and self.workspaces.blobs.root.is_dir()
            ),
        }
        return ReadinessReport(
            ready=all(checks.values()),
            checks=checks,
            schema_version=self.store.schema_version,
            worker_id=self.runner.worker_id,
        )

    def diagnostics(self) -> DiagnosticsReport:
        now = utc_now()
        store_stats = self.store.operational_stats(now)
        runner_stats = self.runner.diagnostics()

        blob_inventory = self.workspaces.blobs.inventory()
        workspaces_total = sum(
            1
            for path in self.workspaces.root.iterdir()
            if path.is_dir() and not path.is_symlink()
        ) if self.workspaces.root.exists() else 0
        _, workspace_bytes = self._directory_stats(self.workspaces.root)

        restores_total = sum(
            1
            for path in self.workspaces.restore_root.iterdir()
            if path.is_dir() and not path.is_symlink()
        ) if self.workspaces.restore_root.exists() else 0
        _, restore_bytes = self._directory_stats(self.workspaces.restore_root)

        database_path = self.root / "execledger.sqlite3"
        wal_path = self.root / "execledger.sqlite3-wal"

        return DiagnosticsReport(
            generated_at=now,
            schema_version=self.store.schema_version,
            worker_id=str(runner_stats["worker_id"]),
            workers_configured=int(runner_stats["workers_configured"]),
            active_processes=int(runner_stats["active_processes"]),
            stopping=bool(runner_stats["stopping"]),
            status_counts=dict(store_stats["status_counts"]),
            queue_ready=int(store_stats["queue_ready"]),
            queue_delayed=int(store_stats["queue_delayed"]),
            active_leases=int(store_stats["active_leases"]),
            expired_leases=int(store_stats["expired_leases"]),
            attempts_total=int(store_stats["attempts_total"]),
            effects_total=int(store_stats["effects_total"]),
            snapshots_total=int(store_stats["snapshots_total"]),
            idempotency_keys=int(store_stats["idempotency_keys"]),
            blobs_total=len(blob_inventory),
            blob_bytes=sum(item.size for item in blob_inventory),
            workspaces_total=workspaces_total,
            workspace_bytes=workspace_bytes,
            restores_total=restores_total,
            restore_bytes=restore_bytes,
            database_bytes=database_path.stat().st_size if database_path.exists() else 0,
            wal_bytes=wal_path.stat().st_size if wal_path.exists() else 0,
        )

    async def start(self) -> None:
        if self._started:
            return
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
