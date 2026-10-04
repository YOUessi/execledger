from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path, PurePosixPath

from execledger.models import ExecutionSpec, SnapshotRecord
from execledger.store import ExecutionStore


class UnsafeWorkspacePath(ValueError):
    pass


class WorkspaceManager:
    def __init__(self, root: Path, store: ExecutionStore):
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.store = store

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

    def snapshot(self, execution_id: str, phase: str) -> SnapshotRecord:
        target = self.directory(execution_id)
        manifest: list[dict[str, object]] = []
        if target.exists():
            for path in sorted(p for p in target.rglob("*") if p.is_file() and not p.is_symlink()):
                relative = path.relative_to(target).as_posix()
                data = path.read_bytes()
                manifest.append(
                    {
                        "path": relative,
                        "size": len(data),
                        "sha256": hashlib.sha256(data).hexdigest(),
                    }
                )
        encoded = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
        digest = hashlib.sha256(encoded).hexdigest()
        return self.store.add_snapshot(execution_id, phase, digest, manifest)
