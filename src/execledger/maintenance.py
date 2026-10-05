from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path

from execledger.blobstore import BlobStore
from execledger.locking import exclusive_file_lock
from execledger.models import GarbageBlob, GarbageCollectionReport, utc_now
from execledger.store import ExecutionStore


class StorageMaintenance:
    def __init__(
        self,
        store: ExecutionStore,
        blobs: BlobStore,
        restore_root: Path,
        lock_path: Path,
    ):
        self.store = store
        self.blobs = blobs
        self.restore_root = restore_root.resolve()
        self.restore_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.lock_path = lock_path

    @staticmethod
    def _directory_size(path: Path) -> int:
        total = 0
        for item in path.rglob("*"):
            if item.is_symlink():
                continue
            if item.is_file():
                total += item.stat().st_size
        return total

    def collect(
        self,
        *,
        dry_run: bool = True,
        restore_older_than_seconds: float | None = None,
        now: datetime | None = None,
    ) -> GarbageCollectionReport:
        if restore_older_than_seconds is not None and restore_older_than_seconds < 0:
            raise ValueError("restore_older_than_seconds must be non-negative")

        now_dt = now or utc_now()
        now_timestamp = now_dt.timestamp()

        with exclusive_file_lock(self.lock_path):
            references, snapshot_count = self.store.snapshot_blob_references()
            inventory = self.blobs.inventory()
            orphans = [item for item in inventory if item.digest not in references]
            orphan_models = [
                GarbageBlob(digest=item.digest, size=item.size)
                for item in orphans
            ]

            deleted_models: list[GarbageBlob] = []
            bytes_reclaimed = 0
            if not dry_run:
                for item in orphans:
                    reclaimed = self.blobs.delete(item.digest)
                    deleted_models.append(
                        GarbageBlob(digest=item.digest, size=reclaimed)
                    )
                    bytes_reclaimed += reclaimed

            restore_dirs: list[Path] = []
            if self.restore_root.exists():
                restore_dirs = sorted(
                    path
                    for path in self.restore_root.iterdir()
                    if path.is_dir() and not path.is_symlink()
                )

            eligible_restores: list[tuple[Path, int]] = []
            if restore_older_than_seconds is not None:
                for path in restore_dirs:
                    age = max(0.0, now_timestamp - path.stat().st_mtime)
                    if age >= restore_older_than_seconds:
                        eligible_restores.append(
                            (path, self._directory_size(path))
                        )

            deleted_restore_names: list[str] = []
            restore_bytes_reclaimed = 0
            if not dry_run:
                for path, size in eligible_restores:
                    shutil.rmtree(path)
                    deleted_restore_names.append(path.name)
                    restore_bytes_reclaimed += size

            return GarbageCollectionReport(
                dry_run=dry_run,
                snapshots_scanned=snapshot_count,
                blobs_scanned=len(inventory),
                referenced_blobs=len(references),
                orphan_blobs=orphan_models,
                deleted_blobs=deleted_models,
                bytes_reclaimable=sum(item.size for item in orphan_models),
                bytes_reclaimed=bytes_reclaimed,
                restores_scanned=len(restore_dirs),
                restore_dirs_eligible=[path.name for path, _ in eligible_restores],
                restore_dirs_deleted=deleted_restore_names,
                restore_bytes_reclaimable=sum(
                    size for _, size in eligible_restores
                ),
                restore_bytes_reclaimed=restore_bytes_reclaimed,
            )
