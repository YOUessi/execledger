from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path

import pytest

from execledger.models import ExecutionSpec, utc_now
from execledger.service import ExecutionService


def _service(tmp_path: Path) -> ExecutionService:
    return ExecutionService(
        tmp_path,
        workers=1,
        lease_seconds=1,
        heartbeat_interval=0.1,
        worker_id="maintenance-test",
    )


def test_gc_dry_run_then_apply_removes_only_unreferenced_blobs(tmp_path: Path):
    service = _service(tmp_path)
    record, _ = service.store.create_execution(
        ExecutionSpec(argv=["echo", "ok"], files={"input.txt": "kept"}),
        "gc-blob-key",
    )
    service.workspaces.prepare(record.id, record.spec)
    snapshot = service.workspaces.snapshot(record.id, "before")

    referenced = str(snapshot.manifest[0]["sha256"])
    orphan = service.workspaces.blobs.put(b"orphan-content")
    assert orphan != referenced
    assert service.workspaces.blobs.contains(referenced)
    assert service.workspaces.blobs.contains(orphan)

    dry = service.maintenance.collect(dry_run=True)
    assert dry.dry_run is True
    assert dry.snapshots_scanned == 1
    assert dry.blobs_scanned == 2
    assert dry.referenced_blobs == 1
    assert [item.digest for item in dry.orphan_blobs] == [orphan]
    assert dry.deleted_blobs == []
    assert dry.bytes_reclaimable == len(b"orphan-content")
    assert dry.bytes_reclaimed == 0
    assert service.workspaces.blobs.contains(orphan)

    applied = service.maintenance.collect(dry_run=False)
    assert applied.dry_run is False
    assert [item.digest for item in applied.deleted_blobs] == [orphan]
    assert applied.bytes_reclaimed == len(b"orphan-content")
    assert not service.workspaces.blobs.contains(orphan)
    assert service.workspaces.blobs.contains(referenced)
    service.store.close()


def test_gc_can_prune_old_restore_directories(tmp_path: Path):
    service = _service(tmp_path)
    record, _ = service.store.create_execution(
        ExecutionSpec(argv=["echo", "ok"], files={"input.txt": "restore-me"}),
        "gc-restore-key",
    )
    service.workspaces.prepare(record.id, record.spec)
    snapshot = service.workspaces.snapshot(record.id, "before")
    restored = service.workspaces.restore(record.id, snapshot.id)
    restore_path = Path(restored.directory)
    assert restore_path.exists()

    now = utc_now()
    old = (now - timedelta(hours=2)).timestamp()
    os.utime(restore_path, (old, old))

    dry = service.maintenance.collect(
        dry_run=True,
        restore_older_than_seconds=3600,
        now=now,
    )
    assert dry.restore_dirs_eligible == [restore_path.name]
    assert dry.restore_dirs_deleted == []
    assert restore_path.exists()

    applied = service.maintenance.collect(
        dry_run=False,
        restore_older_than_seconds=3600,
        now=now,
    )
    assert applied.restore_dirs_deleted == [restore_path.name]
    assert not restore_path.exists()
    service.store.close()


def test_gc_fails_closed_on_invalid_snapshot_manifest(tmp_path: Path):
    service = _service(tmp_path)
    record, _ = service.store.create_execution(
        ExecutionSpec(argv=["echo", "ok"]),
        "gc-invalid-manifest",
    )
    service.workspaces.prepare(record.id, record.spec)
    orphan = service.workspaces.blobs.put(b"do-not-delete")
    service.store.add_snapshot(
        record.id,
        "broken",
        "0" * 64,
        [{"path": "x.txt", "size": 1}],
    )

    with pytest.raises(RuntimeError, match="missing a blob digest"):
        service.maintenance.collect(dry_run=False)

    assert service.workspaces.blobs.contains(orphan)
    service.store.close()


def test_gc_rejects_negative_restore_age(tmp_path: Path):
    service = _service(tmp_path)
    with pytest.raises(ValueError, match="non-negative"):
        service.maintenance.collect(
            dry_run=True,
            restore_older_than_seconds=-1,
        )
    service.store.close()
