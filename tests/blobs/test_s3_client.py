"""``S3BlobStore`` against a fake S3 behind ``httpx.MockTransport``.

No network, no real AWS account: ``FakeS3`` is a small in-memory object store that
answers exactly the requests ``S3BlobStore`` sends (``PUT``/``GET``/``HEAD``/``DELETE`` on
an object, ``GET ?list-type=2`` to list a bucket) and -- this is the point of routing
everything through a fake rather than stubbing responses -- independently re-derives and
checks every request's SigV4 signature using the same low-level functions
``test_sigv4.py`` checks against AWS's vectors. A bug in how :class:`S3BlobStore` builds
the URL, host header, query string or payload hash (as opposed to the SigV4 math itself,
which is already covered) would show up here as a signature mismatch.
"""

from __future__ import annotations

import xml.etree.ElementTree as ElementTree
from dataclasses import dataclass, field

import httpx
import pytest

from loomwork.blobs.errors import BlobNotFoundError, BlobStoreError
from loomwork.blobs.s3 import S3BlobStore, S3Credentials
from loomwork.blobs.sigv4 import SigV4Request, canonical_request, derive_signing_key, sign_string
from tests.blobs.conformance import BlobStoreConformance

BUCKET = "my-bucket"
ACCESS_KEY_ID = "AKIDEXAMPLE"
SECRET_ACCESS_KEY = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
REGION = "us-east-1"

_NS = "http://s3.amazonaws.com/doc/2006-03-01/"


@dataclass
class _StoredObject:
    data: bytes
    metadata: dict[str, str] = field(default_factory=dict)


class FakeS3:
    """An in-memory stand-in for one S3 bucket, with a real SigV4 checker in front."""

    def __init__(self, *, path_style: bool = True) -> None:
        self.objects: dict[str, _StoredObject] = {}
        self.path_style = path_style
        self.requests: list[httpx.Request] = []
        self.next_status: int | None = None  # forces one retryable failure, for retry tests
        self.failures_remaining = 0

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self._handle)

    def _object_key(self, request: httpx.Request) -> str | None:
        path = request.url.path
        if self.path_style:
            prefix = f"/{BUCKET}/"
            if path == f"/{BUCKET}":
                return None
            assert path.startswith(prefix), path
            return path[len(prefix) :]
        assert request.url.host.startswith(f"{BUCKET}.")
        return path[1:] or None

    def _verify_signature(self, request: httpx.Request) -> None:
        auth = request.headers.get("authorization")
        assert auth is not None, "request was not signed"
        assert auth.startswith("AWS4-HMAC-SHA256 Credential=")
        credential_part, signed_headers_part, signature_part = (p.strip() for p in auth.split(","))
        credential = credential_part.removeprefix("AWS4-HMAC-SHA256 Credential=")
        access_key, date_stamp, region, service, _ = credential.split("/")
        assert access_key == ACCESS_KEY_ID
        assert region == REGION
        assert service == "s3"
        signed_header_names = signed_headers_part.removeprefix("SignedHeaders=").split(";")
        expected_signature = signature_part.removeprefix("Signature=")

        headers = {name: request.headers[name] for name in signed_header_names}
        query = request.url.query.decode("ascii")
        sig_request = SigV4Request(
            method=request.method,
            path=request.url.path,
            query=query,
            headers=headers,
            payload=request.content,
        )
        creq, _ = canonical_request(sig_request)
        amz_date = request.headers["x-amz-date"]
        scope = f"{date_stamp}/{region}/{service}/aws4_request"
        from loomwork.blobs.sigv4 import string_to_sign

        sts = string_to_sign(amz_date, scope, creq)
        key = derive_signing_key(SECRET_ACCESS_KEY, date_stamp, region, service)
        actual_signature = sign_string(key, sts)
        assert actual_signature == expected_signature, "SigV4 signature does not verify"

        content_hash = request.headers.get("x-amz-content-sha256")
        assert content_hash is not None

    def _handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        self._verify_signature(request)

        if self.failures_remaining > 0:
            self.failures_remaining -= 1
            return httpx.Response(503, text="SlowDown")

        if "list-type" in request.url.params:
            return self._list(request)

        key = self._object_key(request)
        assert key is not None
        if request.method == "PUT":
            metadata = {
                name.removeprefix("x-amz-meta-"): value
                for name, value in request.headers.items()
                if name.startswith("x-amz-meta-")
            }
            self.objects[key] = _StoredObject(request.content, metadata)
            return httpx.Response(200, headers={"ETag": '"fake-etag"'})
        if request.method in ("GET", "HEAD"):
            obj = self.objects.get(key)
            if obj is None:
                return httpx.Response(404, text="<Error><Code>NoSuchKey</Code></Error>")
            body = b"" if request.method == "HEAD" else obj.data
            return httpx.Response(200, content=body)
        if request.method == "DELETE":
            self.objects.pop(key, None)
            return httpx.Response(204)
        raise AssertionError(f"unexpected method {request.method}")

    def _list(self, request: httpx.Request) -> httpx.Response:
        prefix = request.url.params.get("prefix", "")
        token = request.url.params.get("continuation-token")
        keys = sorted(k for k in self.objects if k.startswith(prefix))
        page_size = 2  # small, so pagination is actually exercised by tests
        start = keys.index(token) if token else 0
        page = keys[start : start + page_size]
        truncated = start + page_size < len(keys)

        root = ElementTree.Element("ListBucketResult", xmlns=_NS)
        for key in page:
            contents = ElementTree.SubElement(root, "Contents")
            ElementTree.SubElement(contents, "Key").text = key
            ElementTree.SubElement(contents, "Size").text = str(len(self.objects[key].data))
            ElementTree.SubElement(contents, "LastModified").text = "2024-01-01T00:00:00.000Z"
        ElementTree.SubElement(root, "IsTruncated").text = "true" if truncated else "false"
        if truncated:
            ElementTree.SubElement(root, "NextContinuationToken").text = page[-1]
        body = ElementTree.tostring(root)
        return httpx.Response(200, content=body)


def make_store(fake: FakeS3, **kwargs: object) -> S3BlobStore:
    return S3BlobStore(
        BUCKET,
        region=REGION,
        endpoint_url="https://fake-s3.example",
        credentials=S3Credentials(ACCESS_KEY_ID, SECRET_ACCESS_KEY),
        client=httpx.AsyncClient(transport=fake.transport()),
        **kwargs,  # type: ignore[arg-type]
    )


@pytest.fixture
def store() -> S3BlobStore:
    return make_store(FakeS3())


class TestS3BlobStoreConformance(BlobStoreConformance):
    pass


async def test_put_then_get_round_trips() -> None:
    fake = FakeS3()
    store = make_store(fake)
    await store.put("reports/1", b"hello s3")
    assert await store.get("reports/1") == b"hello s3"


async def test_get_missing_key_raises_blob_not_found() -> None:
    fake = FakeS3()
    store = make_store(fake)
    with pytest.raises(BlobNotFoundError):
        await store.get("nothing-here")


async def test_exists() -> None:
    fake = FakeS3()
    store = make_store(fake)
    assert not await store.exists("k")
    await store.put("k", b"x")
    assert await store.exists("k")


async def test_delete() -> None:
    fake = FakeS3()
    store = make_store(fake)
    await store.put("k", b"x")
    await store.delete("k")
    assert not await store.exists("k")


async def test_delete_missing_key_is_not_an_error() -> None:
    fake = FakeS3()
    store = make_store(fake)
    await store.delete("never-existed")


async def test_metadata_is_sent_as_amz_meta_headers() -> None:
    fake = FakeS3()
    store = make_store(fake)
    await store.put("k", b"x", metadata={"sha256": "abc123", "size": "1"})
    assert fake.objects["k"].metadata == {"sha256": "abc123", "size": "1"}


async def test_server_side_encryption_header_is_sent() -> None:
    fake = FakeS3()
    store = make_store(fake, server_side_encryption="AES256")
    await store.put("k", b"x")
    put_request = next(r for r in fake.requests if r.method == "PUT")
    assert put_request.headers["x-amz-server-side-encryption"] == "AES256"


async def test_list_paginates_across_continuation_tokens() -> None:
    fake = FakeS3()
    store = make_store(fake)
    for i in range(5):
        await store.put(f"page/{i}", str(i).encode())
    found = {info.key: info.size async for info in store.list("page/")}
    assert found == {f"page/{i}": 1 for i in range(5)}


async def test_list_keys_are_relative_to_the_store_prefix() -> None:
    fake = FakeS3()
    store = make_store(fake, prefix="loomwork/blobs")
    await store.put("abc123", b"data")
    assert "loomwork/blobs/abc123" in fake.objects
    found = [info.key async for info in store.list()]
    assert found == ["abc123"]


async def test_retries_on_5xx_then_succeeds() -> None:
    fake = FakeS3()
    fake.failures_remaining = 2
    store = make_store(fake, max_retries=5, backoff_base=0.001)
    await store.put("k", b"x")
    assert await store.get("k") == b"x"


async def test_gives_up_after_max_retries() -> None:
    fake = FakeS3()
    fake.failures_remaining = 100
    store = make_store(fake, max_retries=2, backoff_base=0.001)
    with pytest.raises(BlobStoreError):
        await store.put("k", b"x")


async def test_virtual_hosted_style_addressing() -> None:
    fake = FakeS3(path_style=False)
    store = S3BlobStore(
        BUCKET,
        region=REGION,
        endpoint_url="https://s3.us-east-1.amazonaws.com",
        credentials=S3Credentials(ACCESS_KEY_ID, SECRET_ACCESS_KEY),
        client=httpx.AsyncClient(transport=fake.transport()),
        path_style=False,
    )
    await store.put("k", b"virtual-style")
    put_request = fake.requests[-1]
    assert put_request.url.host == f"{BUCKET}.s3.us-east-1.amazonaws.com"
    assert await store.get("k") == b"virtual-style"
