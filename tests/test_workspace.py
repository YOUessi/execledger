from pathlib import Path

import pytest

from execledger.models import ExecutionSpec
from execledger.store import ExecutionStore
from execledger.workspace import UnsafeWorkspacePath, WorkspaceManager


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
    assert len(store.snapshots(record.id)) == 2
    store.close()
