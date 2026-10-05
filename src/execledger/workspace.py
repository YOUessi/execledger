from __future__ import annotations

import hashlib
import json
import os
import shutil
import uuid
from pathlib import Path, PurePosixPath

from execledger.blobstore import BlobNotFound, BlobStore, BlobStoreError
from execledger.locking import exclusive_file_lock
from execledger.models import (
    ExecutionSpec,
    FileChange,
    RestoreRecord,
    SnapshotRecord,
    WorkspaceDiff,
)
from execledger.store import ExecutionStore


class UnsafeWorkspacePath(ValueError):
    pass


class SnapshotNotRestorable(RuntimeError):
    pass


class WorkspaceManager:
    def __init__(
        self,
        root: Path,
        store: ExecutionStore,
        *,
        blob_root: Path | None = None,
        restore_root: Path | None = None,
    ):
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.store = store
        state_root = self.root.parent
        self.blobs = BlobStore(blob_root or state_root / "blobs")
        self.maintenance_lock_path = state_root / ".blob-maintenance.lock"
        self.restore_root = (restore_root or state_root / "restores").resolve()
        self.restore_root.mkdir(mode=0o700, parents=True, exist_ok=True)

    @staticmethod
    def validate_relative(path: str) -> PurePosixPath:
        candidate = PurePosixPath(path)
        if (
            not path
            or candidate.is_absolute()
            or any(part in {"", ".", ".."} for part in candidate.parts)
        ):
            raise UnsafeWorkspacePath(path)
        return candidate

    def directory(self, execution_id: str) -> Path:
        return self.root / execution_id

    def prepare(self, execution_id: str, spec: ExecutionSpec) -> Path:
        target = self.directory(execution_id)
        target.mkdir(mode=0o700, parents=True, exist_ok=False)
        for name, content in spec.files.items():
            relative = self.validate_relative(name)
            path = target.joinpath(*relative.parts)
            path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")
            os.chmod(path, 0o600)
        self.store.add_effect(execution_id, "workspace_prepared", {"file_count": len(spec.files)})
        return target

    def usage_bytes(self, execution_id: str) -> int:
        target = self.directory(execution_id)
        if not target.exists():
            return 0
        total = 0
        for path in target.rglob("*"):
            if path.is_symlink():
                continue
            if path.is_file():
                total += path.stat().st_size
        return total

    def snapshot(self, execution_id: str, phase: str) -> SnapshotRecord:
        with exclusive_file_lock(self.maintenance_lock_path):
            target = self.directory(execution_id)
            manifest: list[dict[str, object]] = []
            if target.exists():
                paths = sorted(
                    path
                    for path in target.rglob("*")
                    if path.is_file() and not path.is_symlink()
                )
                for path in paths:
                    relative = path.relative_to(target).as_posix()
                    data = path.read_bytes()
                    digest = self.blobs.put(data)
                    manifest.append(
                        {
                            "path": relative,
                            "size": len(data),
                            "sha256": digest,
                        }
                    )
            encoded = json.dumps(
                manifest,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
            digest = hashlib.sha256(encoded).hexdigest()
            return self.store.add_snapshot(execution_id, phase, digest, manifest)

    def _validated_manifest(
        self,
        snapshot: SnapshotRecord,
    ) -> dict[str, tuple[str, int]]:
        encoded = json.dumps(
            snapshot.manifest,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        if hashlib.sha256(encoded).hexdigest() != snapshot.digest:
            raise SnapshotNotRestorable("snapshot manifest digest mismatch")

        result: dict[str, tuple[str, int]] = {}
        for item in snapshot.manifest:
            if not isinstance(item, dict):
                raise SnapshotNotRestorable("snapshot manifest item is invalid")
            path = item.get("path")
            digest = item.get("sha256")
            size = item.get("size")
            if (
                not isinstance(path, str)
                or not isinstance(digest, str)
                or type(size) is not int
                or size < 0
            ):
                raise SnapshotNotRestorable("snapshot manifest item is invalid")
            self.validate_relative(path)
            if path in result:
                raise SnapshotNotRestorable("snapshot manifest contains duplicate paths")
            result[path] = (digest, size)
        return result

    def diff(
        self,
        execution_id: str,
        before_snapshot_id: str,
        after_snapshot_id: str,
    ) -> WorkspaceDiff:
        before = self.store.get_snapshot(execution_id, before_snapshot_id)
        after = self.store.get_snapshot(execution_id, after_snapshot_id)
        before_map = self._validated_manifest(before)
        after_map = self._validated_manifest(after)

        added: list[FileChange] = []
        modified: list[FileChange] = []
        deleted: list[FileChange] = []

        for path in sorted(set(before_map) | set(after_map)):
            old = before_map.get(path)
            new = after_map.get(path)
            if old is None and new is not None:
                added.append(
                    FileChange(
                        path=path,
                        after_sha256=new[0],
                        after_size=new[1],
                    )
                )
            elif new is None and old is not None:
                deleted.append(
                    FileChange(
                        path=path,
                        before_sha256=old[0],
                        before_size=old[1],
                    )
                )
            elif old is not None and new is not None and old != new:
                modified.append(
                    FileChange(
                        path=path,
                        before_sha256=old[0],
                        after_sha256=new[0],
                        before_size=old[1],
                        after_size=new[1],
                    )
                )

        return WorkspaceDiff(
            before_snapshot_id=before.id,
            after_snapshot_id=after.id,
            added=added,
            modified=modified,
            deleted=deleted,
        )

    def restore(self, execution_id: str, snapshot_id: str) -> RestoreRecord:
        with exclusive_file_lock(self.maintenance_lock_path):
            snapshot = self.store.get_snapshot(execution_id, snapshot_id)
            manifest = self._validated_manifest(snapshot)
            restore_id = uuid.uuid4().hex
            target = self.restore_root / restore_id
            target.mkdir(mode=0o700, parents=False, exist_ok=False)

            try:
                for name, (digest, expected_size) in sorted(manifest.items()):
                    relative = self.validate_relative(name)
                    try:
                        data = self.blobs.get(digest)
                    except (BlobNotFound, BlobStoreError) as exc:
                        raise SnapshotNotRestorable(
                            f"snapshot blob unavailable: {digest}"
                        ) from exc
                    if len(data) != expected_size:
                        raise SnapshotNotRestorable(
                            f"snapshot blob size mismatch: {digest}"
                        )
                    path = target.joinpath(*relative.parts)
                    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                    path.write_bytes(data)
                    os.chmod(path, 0o600)
            except Exception:
                shutil.rmtree(target, ignore_errors=True)
                raise

            self.store.add_effect(
                execution_id,
                "snapshot_restored",
                {
                    "snapshot_id": snapshot.id,
                    "restore_id": restore_id,
                    "file_count": len(manifest),
                },
            )
            return RestoreRecord(
                id=restore_id,
                execution_id=execution_id,
                snapshot_id=snapshot.id,
                directory=str(target),
                file_count=len(manifest),
                snapshot_digest=snapshot.digest,
            )
