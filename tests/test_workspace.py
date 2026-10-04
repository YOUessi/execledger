import hashlib
import json
from pathlib import Path

import pytest

from execledger.models import ExecutionSpec
from execledger.store import ExecutionStore
from execledger.workspace import (
    SnapshotNotRestorable,
    UnsafeWorkspacePath,
    WorkspaceManager,
)


def _manifest_digest(manifest: list[dict[str, object]]) -> str:
    encoded = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def test_workspace_rejects_escape_paths(tmp_path: Path):
    store = ExecutionStore(tmp_path / "db.sqlite3")
    manager = WorkspaceManager(tmp_path / "workspaces", store)
    for path in ("../secret", "/tmp/secret", "a/../../b", ""):
        with pytest.raises(UnsafeWorkspacePath):
            manager.validate_relative(path)
    store.close()


def test_prepare_permissions_and_snapshot(tmp_path: Path):
    store = ExecutionStore(tmp_path / "db.sqlite3")
    manager = WorkspaceManager(tmp_path / "workspaces", store)
    record, _ = store.create_execution(
        ExecutionSpec(argv=["echo", "ok"], files={"src/a.txt": "hello"}),
        "k",
    )
    directory = manager.prepare(record.id, record.spec)
    path = directory / "src" / "a.txt"
    assert path.read_text() == "hello"
    assert path.stat().st_mode & 0o777 == 0o600
    snap1 = manager.snapshot(record.id, "before")
    path.write_text("changed")
    snap2 = manager.snapshot(record.id, "after")
    assert snap1.digest != snap2.digest
    assert snap1.manifest[0]["path"] == "src/a.txt"
    assert manager.blobs.get(str(snap1.manifest[0]["sha256"])) == b"hello"
    assert len(store.snapshots(record.id)) == 2
    store.close()


def test_snapshot_diff_and_restore_round_trip(tmp_path: Path):
    store = ExecutionStore(tmp_path / "db.sqlite3")
    manager = WorkspaceManager(tmp_path / "workspaces", store)
    record, _ = store.create_execution(
        ExecutionSpec(
            argv=["echo", "ok"],
            files={
                "src/a.txt": "before",
                "keep.txt": "delete-me",
            },
        ),
        "restore-key",
    )
    directory = manager.prepare(record.id, record.spec)
    before = manager.snapshot(record.id, "before")

    (directory / "src" / "a.txt").write_text("after")
    (directory / "keep.txt").unlink()
    (directory / "new.txt").write_text("created")
    after = manager.snapshot(record.id, "after")

    diff = manager.diff(record.id, before.id, after.id)
    assert [item.path for item in diff.added] == ["new.txt"]
    assert [item.path for item in diff.modified] == ["src/a.txt"]
    assert [item.path for item in diff.deleted] == ["keep.txt"]

    restored_before = manager.restore(record.id, before.id)
    before_root = Path(restored_before.directory)
    assert (before_root / "src" / "a.txt").read_text() == "before"
    assert (before_root / "keep.txt").read_text() == "delete-me"
    assert not (before_root / "new.txt").exists()

    restored_after = manager.restore(record.id, after.id)
    after_root = Path(restored_after.directory)
    assert (after_root / "src" / "a.txt").read_text() == "after"
    assert (after_root / "new.txt").read_text() == "created"
    assert not (after_root / "keep.txt").exists()

    assert restored_before.snapshot_digest == before.digest
    assert restored_after.snapshot_digest == after.digest
    assert [e.kind for e in store.effects(record.id)].count("snapshot_restored") == 2
    store.close()


def test_restore_rejects_manifest_path_escape(tmp_path: Path):
    store = ExecutionStore(tmp_path / "db.sqlite3")
    manager = WorkspaceManager(tmp_path / "workspaces", store)
    record, _ = store.create_execution(
        ExecutionSpec(argv=["echo", "ok"]),
        "escape-restore",
    )
    manager.prepare(record.id, record.spec)
    digest = manager.blobs.put(b"secret")
    manifest = [{"path": "../escape.txt", "size": 6, "sha256": digest}]
    snapshot = store.add_snapshot(
        record.id,
        "manual",
        _manifest_digest(manifest),
        manifest,
    )

    with pytest.raises(UnsafeWorkspacePath):
        manager.restore(record.id, snapshot.id)
    store.close()


def test_legacy_snapshot_without_blob_fails_cleanly(tmp_path: Path):
    store = ExecutionStore(tmp_path / "db.sqlite3")
    manager = WorkspaceManager(tmp_path / "workspaces", store)
    record, _ = store.create_execution(
        ExecutionSpec(argv=["echo", "ok"]),
        "legacy-restore",
    )
    manager.prepare(record.id, record.spec)

    missing = hashlib.sha256(b"not-stored").hexdigest()
    manifest = [{"path": "old.txt", "size": 10, "sha256": missing}]
    snapshot = store.add_snapshot(
        record.id,
        "legacy",
        _manifest_digest(manifest),
        manifest,
    )

    with pytest.raises(SnapshotNotRestorable, match="blob unavailable"):
        manager.restore(record.id, snapshot.id)

    restore_entries = list((tmp_path / "restores").iterdir())
    assert restore_entries == []
    store.close()
