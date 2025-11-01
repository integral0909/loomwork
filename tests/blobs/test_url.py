from __future__ import annotations

from pathlib import Path

import pytest

from loomwork.blobs.errors import BlobStoreError
from loomwork.blobs.filesystem import FileSystemBlobStore
from loomwork.blobs.memory import MemoryBlobStore
from loomwork.blobs.s3 import S3BlobStore
from loomwork.blobs.url import blob_store_from_url


def test_memory_url() -> None:
    store = blob_store_from_url("memory://")
    assert isinstance(store, MemoryBlobStore)


def test_memory_url_instances_are_independent() -> None:
    a = blob_store_from_url("memory://")
    b = blob_store_from_url("memory://")
    assert a is not b


def test_file_url(tmp_path: Path) -> None:
    store = blob_store_from_url(f"file://{tmp_path}/blobs")
    assert isinstance(store, FileSystemBlobStore)
    assert store.root == tmp_path / "blobs"


def test_s3_url_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIDEXAMPLE")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
    store = blob_store_from_url("s3://my-bucket/some/prefix")
    assert isinstance(store, S3BlobStore)
    assert store.bucket == "my-bucket"
    assert store.prefix == "some/prefix/"
    assert store.region == "us-east-1"


def test_s3_url_with_region_and_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIDEXAMPLE")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
    store = blob_store_from_url(
        "s3://my-bucket?region=eu-west-1&endpoint=http://localhost:9000&path_style=true&sse=AES256"
    )
    assert isinstance(store, S3BlobStore)
    assert store.region == "eu-west-1"
    assert store._endpoint_url == "http://localhost:9000"
    assert store._path_style is True
    assert store.server_side_encryption == "AES256"


def test_s3_url_without_bucket_raises() -> None:
    with pytest.raises(BlobStoreError, match="bucket"):
        blob_store_from_url("s3:///no-bucket-here")


def test_s3_url_never_reads_credentials_from_the_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    with pytest.raises(BlobStoreError, match="AWS_ACCESS_KEY_ID"):
        blob_store_from_url("s3://my-bucket?access_key=AKIDEXAMPLE&secret_key=whatever")


def test_unsupported_scheme_raises() -> None:
    with pytest.raises(BlobStoreError, match="scheme"):
        blob_store_from_url("ftp://nope")
