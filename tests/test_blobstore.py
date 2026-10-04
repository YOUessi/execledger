from pathlib import Path

import pytest

from execledger.blobstore import BlobCorruption, BlobNotFound, BlobStore


def test_blob_store_deduplicates_identical_content(tmp_path: Path):
    store = BlobStore(tmp_path / "blobs")
    first = store.put(b"same-content")
    second = store.put(b"same-content")

    assert first == second
    assert store.get(first) == b"same-content"
    files = [path for path in (tmp_path / "blobs").rglob("*") if path.is_file()]
    assert files == [store.path_for(first)]


def test_blob_store_rejects_missing_and_corrupted_content(tmp_path: Path):
    store = BlobStore(tmp_path / "blobs")
    digest = store.put(b"original")
    target = store.path_for(digest)
    target.write_bytes(b"corrupted")

    with pytest.raises(BlobCorruption):
        store.get(digest)

    target.unlink()
    with pytest.raises(BlobNotFound):
        store.get(digest)


@pytest.mark.parametrize("digest", ["", "../escape", "g" * 64, "0" * 63])
def test_blob_store_rejects_invalid_digest(digest: str, tmp_path: Path):
    store = BlobStore(tmp_path / "blobs")
    with pytest.raises(ValueError):
        store.path_for(digest)
