"""A from-scratch implementation of AWS Signature Version 4 request signing.

No ``boto3`` or ``botocore`` dependency: :class:`S3BlobStore` needs to sign a handful of
simple, unsigned-payload HTTP requests (``PUT``/``GET``/``HEAD``/``DELETE`` on an object,
``GET`` with a query string to list a bucket), which is a small enough slice of the
SigV4 spec to implement and verify directly. ``tests/blobs/test_sigv4.py`` checks the
low-level functions in this module (:func:`canonical_request`, :func:`string_to_sign`,
:func:`derive_signing_key`, :func:`sign_string`) against several examples from AWS's
published Signature Version 4 test suite, which fixes the credentials, date and expected
canonical request / string to sign / signature for each one -- so a subtle bug in
canonical-request construction (query string ordering, header value trimming, URI
encoding, ...) is caught independently of anything :class:`S3BlobStore` itself asserts.

Reference: `Elements of an AWS API request signature
<https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-signing-elements.html>`_.
"""

from __future__ import annotations

import hashlib
import hmac
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from urllib.parse import parse_qsl, quote

ALGORITHM = "AWS4-HMAC-SHA256"

_EMPTY_PAYLOAD_HASH = hashlib.sha256(b"").hexdigest()


def payload_sha256(payload: bytes) -> str:
    """The lowercase hex SHA-256 digest of a request body, including the empty case."""
    if not payload:
        return _EMPTY_PAYLOAD_HASH
    return hashlib.sha256(payload).hexdigest()


def _uri_encode(value: str) -> str:
    """Percent-encode every byte except the SigV4 unreserved set (``A-Za-z0-9-._~``).

    ``urllib.parse.quote`` with an empty ``safe`` set implements exactly this: it
    always leaves ``-._~`` unescaped, escapes everything else (including ``/``) as
    uppercase ``%XX`` of each UTF-8 byte, and never uses ``+`` for space -- all
    properties the SigV4 spec requires and generic percent-encoders do not always have.
    """
    return quote(value, safe="")


def canonical_uri(path: str) -> str:
    """URI-encode ``path`` one segment at a time, so ``/`` separators stay literal.

    Segments are *not* normalised (``.``/``..`` collapsed): object keys may legitimately
    contain them, which is why S3's own signer does not normalise either.
    """
    if not path:
        return "/"
    return "/".join(_uri_encode(segment) for segment in path.split("/"))


def canonical_query_string(query: str) -> str:
    """URI-encode and sort ``query`` (a raw ``a=1&b=2`` string, no leading ``?``).

    Sorting happens on the *encoded* key and then value, matching the spec exactly
    (this only matters when it disagrees with sorting the raw strings, as it does for
    query keys that use characters quote encodes).
    """
    if not query:
        return ""
    pairs = parse_qsl(query, keep_blank_values=True)
    encoded = sorted((_uri_encode(k), _uri_encode(v)) for k, v in pairs)
    return "&".join(f"{k}={v}" for k, v in encoded)


def _trim_header_value(value: str) -> str:
    """Trim leading/trailing whitespace and collapse interior whitespace runs to one
    space -- applied to the raw value, including any quotes it carries."""
    return " ".join(value.split())


def canonical_headers(headers: Mapping[str, str]) -> tuple[str, str]:
    """Return ``(canonical_headers, signed_headers)``.

    Header names sharing a lowercased spelling are folded together (comma-joined, in
    the order they were given) -- this only matters for header names repeated with
    different casing, since a plain ``dict`` cannot carry the same key twice.
    """
    grouped: dict[str, list[str]] = {}
    for name, value in headers.items():
        grouped.setdefault(name.lower(), []).append(_trim_header_value(value))
    names = sorted(grouped)
    canonical = "".join(f"{name}:{','.join(grouped[name])}\n" for name in names)
    return canonical, ";".join(names)


@dataclass(frozen=True, slots=True)
class SigV4Request:
    """The pieces of an HTTP request that feed into its canonical form."""

    method: str
    path: str
    query: str = ""
    headers: Mapping[str, str] = field(default_factory=dict)
    payload: bytes = b""


def canonical_request(request: SigV4Request) -> tuple[str, str]:
    """Return ``(canonical_request, signed_headers)`` for ``request``."""
    headers_block, signed_headers = canonical_headers(request.headers)
    parts = (
        request.method.upper(),
        canonical_uri(request.path),
        canonical_query_string(request.query),
        headers_block,
        signed_headers,
        payload_sha256(request.payload),
    )
    return "\n".join(parts), signed_headers


def string_to_sign(amz_date: str, credential_scope: str, canonical_req: str) -> str:
    hashed = hashlib.sha256(canonical_req.encode("utf-8")).hexdigest()
    return f"{ALGORITHM}\n{amz_date}\n{credential_scope}\n{hashed}"


def _hmac(key: bytes, message: str) -> bytes:
    return hmac.new(key, message.encode("utf-8"), hashlib.sha256).digest()


def derive_signing_key(secret_access_key: str, date_stamp: str, region: str, service: str) -> bytes:
    """The nested-HMAC signing key: secret -> date -> region -> service -> ``aws4_request``.

    Deriving a key scoped this narrowly (one calendar day, one region, one service) is
    what lets AWS revoke a leaked *derived* key's usefulness within a day even though the
    long-lived secret access key it came from is unaffected.
    """
    date_key = _hmac(f"AWS4{secret_access_key}".encode(), date_stamp)
    region_key = _hmac(date_key, region)
    service_key = _hmac(region_key, service)
    return _hmac(service_key, "aws4_request")


def sign_string(signing_key: bytes, to_sign: str) -> str:
    return hmac.new(signing_key, to_sign.encode("utf-8"), hashlib.sha256).hexdigest()


def credential_scope(date_stamp: str, region: str, service: str) -> str:
    return f"{date_stamp}/{region}/{service}/aws4_request"


@dataclass(frozen=True, slots=True)
class SigV4Signer:
    """Signs requests for one set of credentials, region and service.

    :meth:`sign` is what :class:`~loomwork.blobs.s3.S3BlobStore` actually uses: it adds
    the headers S3 requests need (``x-amz-date``, ``x-amz-content-sha256``, and
    ``x-amz-security-token`` for temporary credentials) before delegating to the
    lower-level, independently-tested functions in this module.
    """

    access_key_id: str
    secret_access_key: str
    region: str
    service: str = "s3"
    session_token: str | None = None

    def sign(
        self,
        *,
        method: str,
        path: str,
        query: str = "",
        headers: Mapping[str, str],
        payload: bytes = b"",
        timestamp: datetime,
    ) -> dict[str, str]:
        """Return ``headers`` plus every header SigV4 authentication adds."""
        amz_date = timestamp.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = timestamp.strftime("%Y%m%d")
        signed = dict(headers)
        signed["x-amz-date"] = amz_date
        signed["x-amz-content-sha256"] = payload_sha256(payload)
        if self.session_token:
            signed["x-amz-security-token"] = self.session_token
        request = SigV4Request(
            method=method, path=path, query=query, headers=signed, payload=payload
        )
        creq, signed_header_names = canonical_request(request)
        scope = credential_scope(date_stamp, self.region, self.service)
        to_sign = string_to_sign(amz_date, scope, creq)
        key = derive_signing_key(self.secret_access_key, date_stamp, self.region, self.service)
        signature = sign_string(key, to_sign)
        signed["authorization"] = (
            f"{ALGORITHM} Credential={self.access_key_id}/{scope}, "
            f"SignedHeaders={signed_header_names}, Signature={signature}"
        )
        return signed
