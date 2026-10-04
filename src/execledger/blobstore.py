from __future__ import annotations

import hashlib
import os
import re
import uuid
from pathlib import Path

_DIGEST = re.compile(r"^[0-9a-f]{64}$")


class BlobStoreError(RuntimeError):
    pass


class BlobNotFound(BlobStoreError):
    pass


class BlobCorruption(BlobStoreError):
    pass


class BlobStore:
    """Small content-addressed byte store rooted under the ExecLedger state directory."""

    def __init__(self, root: Path):
        self.root = root.resolve()
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)

    @staticmethod
    def _validate_digest(digest: str) -> str:
        if not _DIGEST.fullmatch(digest):
            raise ValueError("invalid sha256 digest")
        return digest

    def path_for(self, digest: str) -> Path:
        digest = self._validate_digest(digest)
        return self.root / digest[:2] / digest

    def put(self, data: bytes) -> str:
        digest = hashlib.sha256(data).hexdigest()
        target = self.path_for(digest)
        target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)

        if target.exists() or target.is_symlink():
            if target.is_symlink() or not target.is_file():
                raise BlobCorruption(f"blob path is not a regular file: {digest}")
            existing = target.read_bytes()
            if hashlib.sha256(existing).hexdigest() != digest:
                raise BlobCorruption(f"blob digest mismatch: {digest}")
            return digest

        temporary = target.parent / f".{digest}.{uuid.uuid4().hex}.tmp"
        try:
            with temporary.open("xb") as handle:
                os.chmod(temporary, 0o600)
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, target)
        finally:
            if temporary.exists():
                temporary.unlink()

        stored = target.read_bytes()
        if hashlib.sha256(stored).hexdigest() != digest:
            raise BlobCorruption(f"blob digest mismatch after write: {digest}")
        return digest

    def get(self, digest: str) -> bytes:
        target = self.path_for(digest)
        if target.is_symlink() or not target.is_file():
            raise BlobNotFound(digest)
        data = target.read_bytes()
        if hashlib.sha256(data).hexdigest() != digest:
            raise BlobCorruption(f"blob digest mismatch: {digest}")
        return data

    def contains(self, digest: str) -> bool:
        try:
            self.get(digest)
        except BlobNotFound:
            return False
        return True
