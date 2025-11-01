"""Blob storage on S3 or an S3-compatible endpoint (MinIO, Cloudflare R2, ...).

Needs the optional ``httpx`` dependency (the ``s3`` extra); nothing else in
``loomwork.blobs`` does, which is why this module is imported lazily from
``loomwork.blobs.__init__`` and from :func:`~loomwork.blobs.url.blob_store_from_url`.
Requests are signed with the hand-written SigV4 implementation in
``loomwork.blobs.sigv4`` -- no ``boto3``.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator, Mapping
from datetime import UTC, datetime
from types import TracebackType
from urllib.parse import quote
from xml.etree import ElementTree

import httpx

from loomwork.blobs.base import BlobInfo
from loomwork.blobs.errors import BlobNotFoundError, BlobStoreError
from loomwork.blobs.sigv4 import SigV4Signer

#: Statuses worth a retry with backoff: throttling and transient server errors. Anything
#: else (including 404, which callers interpret themselves) is returned immediately.
_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})

_LIST_XML_NAMESPACE = "{http://s3.amazonaws.com/doc/2006-03-01/}"

DEFAULT_MAX_RETRIES = 5
DEFAULT_BACKOFF_BASE = 0.2
DEFAULT_TIMEOUT = 30.0


class S3Credentials:
    """Access key, secret key and optional session token."""

    __slots__ = ("access_key_id", "secret_access_key", "session_token")

    def __init__(
        self, access_key_id: str, secret_access_key: str, session_token: str | None = None
    ) -> None:
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.session_token = session_token

    def __repr__(self) -> str:
        return f"S3Credentials(access_key_id={self.access_key_id!r}, secret_access_key=<redacted>)"


def credentials_from_env(env: Mapping[str, str] | None = None) -> S3Credentials:
    """Read ``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY`` / ``AWS_SESSION_TOKEN``.

    The standard AWS SDK environment variables; never logged, never accepted through a
    ``s3://`` URL (see ``loomwork.blobs.url``) precisely so they cannot end up in a log
    line or a stored connection string.
    """
    source = env if env is not None else os.environ
    access_key = source.get("AWS_ACCESS_KEY_ID")
    secret_key = source.get("AWS_SECRET_ACCESS_KEY")
    if not access_key or not secret_key:
        raise BlobStoreError(
            "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set to use S3BlobStore "
            "without explicit credentials"
        )
    return S3Credentials(access_key, secret_key, source.get("AWS_SESSION_TOKEN") or None)


def _raise_for_status(response: httpx.Response, key: str) -> None:
    if 200 <= response.status_code < 300:
        return
    raise BlobStoreError(
        f"S3 request for {key!r} failed with status {response.status_code}: "
        f"{response.text[:500]!r}"
    )


class S3BlobStore:
    """Stores blobs as objects in one S3 (or S3-compatible) bucket, under ``prefix``.

    Args:
        bucket: bucket name.
        region: AWS region (or the region an S3-compatible endpoint expects in its
            signing scope; consult its docs -- MinIO and R2 both accept ``auto`` or a
            placeholder region).
        prefix: prepended to every key, with a trailing ``/`` added if missing.
        endpoint_url: set for anything that is not AWS S3 itself (``http://localhost:9000``
            for MinIO, ``https://<account>.r2.cloudflarestorage.com`` for R2, ...).
        credentials: defaults to :func:`credentials_from_env`.
        path_style: ``bucket`` as a path segment (``endpoint/bucket/key``) instead of a
            virtual-hosted subdomain (``bucket.endpoint/key``). Defaults to ``True`` when
            ``endpoint_url`` is given (most self-hosted endpoints don't have the
            wildcard DNS virtual-hosted addressing needs) and ``False`` for AWS S3.
        server_side_encryption: sets ``x-amz-server-side-encryption`` on every ``put``
            (for example ``"AES256"`` or ``"aws:kms"``).
        max_retries: additional attempts after the first, for a 5xx or throttling
            response, or a transport-level failure. Backed off with
            ``backoff_base * 2 ** attempt``.
    """

    def __init__(
        self,
        bucket: str,
        *,
        region: str = "us-east-1",
        prefix: str = "",
        endpoint_url: str | None = None,
        credentials: S3Credentials | None = None,
        path_style: bool | None = None,
        server_side_encryption: str | None = None,
        max_retries: int = DEFAULT_MAX_RETRIES,
        backoff_base: float = DEFAULT_BACKOFF_BASE,
        timeout: float = DEFAULT_TIMEOUT,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.bucket = bucket
        self.region = region
        self.prefix = f"{prefix}/" if prefix and not prefix.endswith("/") else prefix
        self.server_side_encryption = server_side_encryption
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        credentials = credentials or credentials_from_env()
        self._signer = SigV4Signer(
            access_key_id=credentials.access_key_id,
            secret_access_key=credentials.secret_access_key,
            session_token=credentials.session_token,
            region=region,
            service="s3",
        )
        self._endpoint_url = endpoint_url.rstrip("/") if endpoint_url else None
        self._path_style = path_style if path_style is not None else endpoint_url is not None
        self._client = client or httpx.AsyncClient(timeout=timeout)
        self._owns_client = client is None

    async def __aenter__(self) -> S3BlobStore:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    # ------------------------------------------------------------------ addressing

    def _full_key(self, key: str) -> str:
        return f"{self.prefix}{key}" if self.prefix else key

    def _base_url(self) -> tuple[str, str]:
        """``(scheme://host, path_prefix)``; ``path_prefix`` holds ``/bucket`` for
        path-style addressing, or is empty when the bucket is already in the host."""
        scheme_host = self._endpoint_url or f"https://s3.{self.region}.amazonaws.com"
        if self._path_style:
            return scheme_host, f"/{self.bucket}"
        scheme, _, host = scheme_host.partition("://")
        return f"{scheme}://{self.bucket}.{host}", ""

    def _url_for(self, object_key: str | None) -> tuple[str, str, str]:
        base, path_prefix = self._base_url()
        _, _, host = base.partition("://")
        path = f"{path_prefix}/{object_key}" if object_key else path_prefix or "/"
        return f"{base}{path}", host, path

    # ------------------------------------------------------------------ transport

    async def _request(
        self,
        method: str,
        object_key: str | None,
        *,
        query: str = "",
        headers: Mapping[str, str] | None = None,
        body: bytes = b"",
    ) -> httpx.Response:
        url, host, path = self._url_for(object_key)
        base_headers = dict(headers or {})
        base_headers["host"] = host
        request_url = f"{url}?{query}" if query else url
        attempt = 0
        while True:
            signed = self._signer.sign(
                method=method,
                path=path,
                query=query,
                headers=base_headers,
                payload=body,
                timestamp=datetime.now(UTC),
            )
            try:
                response = await self._client.request(
                    method, request_url, headers=signed, content=body
                )
            except httpx.TransportError as exc:
                if attempt >= self.max_retries:
                    raise BlobStoreError(
                        f"S3 request failed: {method} {object_key or '/'}: {exc}"
                    ) from exc
            else:
                if response.status_code not in _RETRYABLE_STATUS or attempt >= self.max_retries:
                    return response
            await asyncio.sleep(self.backoff_base * (2**attempt))
            attempt += 1

    # ------------------------------------------------------------------ BlobStore

    async def put(self, key: str, data: bytes, metadata: Mapping[str, str] | None = None) -> None:
        headers = {f"x-amz-meta-{name}": value for name, value in (metadata or {}).items()}
        if self.server_side_encryption:
            headers["x-amz-server-side-encryption"] = self.server_side_encryption
        response = await self._request(
            "PUT", self._full_key(key), headers=headers, body=bytes(data)
        )
        _raise_for_status(response, key)

    async def get(self, key: str) -> bytes:
        response = await self._request("GET", self._full_key(key))
        if response.status_code == 404:
            raise BlobNotFoundError(key)
        _raise_for_status(response, key)
        return response.content

    async def delete(self, key: str) -> None:
        response = await self._request("DELETE", self._full_key(key))
        if response.status_code in (200, 202, 204, 404):
            return
        _raise_for_status(response, key)

    async def exists(self, key: str) -> bool:
        response = await self._request("HEAD", self._full_key(key))
        if response.status_code == 404:
            return False
        _raise_for_status(response, key)
        return True

    async def list(self, prefix: str = "") -> AsyncIterator[BlobInfo]:
        full_prefix = self._full_key(prefix)
        token: str | None = None
        while True:
            params = {"list-type": "2", "prefix": full_prefix}
            if token:
                params["continuation-token"] = token
            query = "&".join(
                f"{quote(k, safe='')}={quote(v, safe='')}" for k, v in sorted(params.items())
            )
            response = await self._request("GET", None, query=query)
            _raise_for_status(response, prefix)
            root = ElementTree.fromstring(response.content)  # noqa: S314 - our own bucket's response
            for item in root.findall(f"{_LIST_XML_NAMESPACE}Contents"):
                info = self._parse_listed_object(item)
                if info is not None:
                    yield info
            truncated = root.findtext(f"{_LIST_XML_NAMESPACE}IsTruncated")
            token = root.findtext(f"{_LIST_XML_NAMESPACE}NextContinuationToken")
            if truncated != "true" or not token:
                return

    def _parse_listed_object(self, item: ElementTree.Element) -> BlobInfo | None:
        full_key = item.findtext(f"{_LIST_XML_NAMESPACE}Key")
        if not full_key:
            return None
        key = (
            full_key[len(self.prefix) :]
            if self.prefix and full_key.startswith(self.prefix)
            else full_key
        )
        size_text = item.findtext(f"{_LIST_XML_NAMESPACE}Size")
        modified_text = item.findtext(f"{_LIST_XML_NAMESPACE}LastModified")
        last_modified = (
            datetime.fromisoformat(modified_text.replace("Z", "+00:00"))
            if modified_text
            else datetime.now(UTC)
        )
        return BlobInfo(
            key=key, size=int(size_text) if size_text else 0, last_modified=last_modified
        )
