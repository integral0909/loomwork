"""``loomwork.blobs.sigv4`` against AWS's published Signature Version 4 test suite.

Vectors below are taken from the "get-vanilla" family of the suite AWS publishes at
https://docs.aws.amazon.com/general/latest/gr/signature-v4-test-suite.html (mirrored,
among other places, at https://github.com/mongodb/libmongocrypt under
``kms-message/aws-sig-v4-test-suite/``). Every vector uses the same fixed test
credentials, region, service and date, reproduced here verbatim:

* access key id: ``AKIDEXAMPLE``
* secret access key: ``wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY``
* region: ``us-east-1``, service: ``service`` (the suite's placeholder service, not
  ``s3`` -- :class:`~loomwork.blobs.s3.S3BlobStore` is exercised separately, against a
  fake S3 behind ``httpx.MockTransport``, in ``test_s3_client.py``)
* date: 2015-08-30, ``X-Amz-Date: 20150830T123600Z``

For each request this checks the exact canonical request, the SHA-256 hash embedded in
the string to sign, and the final signature -- three independent checkpoints, so a test
failure points at which stage (canonical form, hashing, or HMAC key derivation) is wrong.
"""

from __future__ import annotations

import pytest

from loomwork.blobs.sigv4 import (
    SigV4Request,
    canonical_request,
    credential_scope,
    derive_signing_key,
    sign_string,
    string_to_sign,
)

ACCESS_KEY_ID = "AKIDEXAMPLE"
SECRET_ACCESS_KEY = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
REGION = "us-east-1"
SERVICE = "service"
DATE_STAMP = "20150830"
AMZ_DATE = "20150830T123600Z"

_EMPTY_BODY_HASH = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

# name, request, expected canonical request, expected string-to-sign hash, expected signature
VECTORS = [
    (
        "get-vanilla",
        SigV4Request(
            method="GET",
            path="/",
            headers={"Host": "example.amazonaws.com", "X-Amz-Date": AMZ_DATE},
        ),
        "\n".join(
            [
                "GET",
                "/",
                "",
                "host:example.amazonaws.com",
                f"x-amz-date:{AMZ_DATE}",
                "",
                "host;x-amz-date",
                _EMPTY_BODY_HASH,
            ]
        ),
        "bb579772317eb040ac9ed261061d46c1f17a8133879d6129b6e1c25292927e63",
        "5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31",
    ),
    (
        "get-vanilla-query-order-key",
        SigV4Request(
            method="GET",
            path="/",
            query="Param1=value2&Param1=Value1",
            headers={"Host": "example.amazonaws.com", "X-Amz-Date": AMZ_DATE},
        ),
        "\n".join(
            [
                "GET",
                "/",
                "Param1=Value1&Param1=value2",
                "host:example.amazonaws.com",
                f"x-amz-date:{AMZ_DATE}",
                "",
                "host;x-amz-date",
                _EMPTY_BODY_HASH,
            ]
        ),
        "704b4cef673542d84cdff252633f065e8daeba5f168b77116f8b1bcaf3d38f89",
        "eedbc4e291e521cf13422ffca22be7d2eb8146eecf653089df300a15b2382bd1",
    ),
    (
        "get-vanilla-query-order-value",
        SigV4Request(
            method="GET",
            path="/",
            query="Param1=value2&Param1=value1",
            headers={"Host": "example.amazonaws.com", "X-Amz-Date": AMZ_DATE},
        ),
        "\n".join(
            [
                "GET",
                "/",
                "Param1=value1&Param1=value2",
                "host:example.amazonaws.com",
                f"x-amz-date:{AMZ_DATE}",
                "",
                "host;x-amz-date",
                _EMPTY_BODY_HASH,
            ]
        ),
        "c968629d70850097a2d8781c9bf7edcb988b04cac14cca9be4acc3595f884606",
        "5772eed61e12b33fae39ee5e7012498b51d56abc0abb7c60486157bd471c4694",
    ),
    (
        "get-vanilla-empty-query-key",
        SigV4Request(
            method="GET",
            path="/",
            query="Param1=value1",
            headers={"Host": "example.amazonaws.com", "X-Amz-Date": AMZ_DATE},
        ),
        "\n".join(
            [
                "GET",
                "/",
                "Param1=value1",
                "host:example.amazonaws.com",
                f"x-amz-date:{AMZ_DATE}",
                "",
                "host;x-amz-date",
                _EMPTY_BODY_HASH,
            ]
        ),
        "1e24db194ed7d0eec2de28d7369675a243488e08526e8c1c73571282f7c517ab",
        "a67d582fa61cc504c4bae71f336f98b97f1ea3c7a6bfe1b6e45aec72011b9aeb",
    ),
    (
        "post-vanilla",
        SigV4Request(
            method="POST",
            path="/",
            headers={"Host": "example.amazonaws.com", "X-Amz-Date": AMZ_DATE},
        ),
        "\n".join(
            [
                "POST",
                "/",
                "",
                "host:example.amazonaws.com",
                f"x-amz-date:{AMZ_DATE}",
                "",
                "host;x-amz-date",
                _EMPTY_BODY_HASH,
            ]
        ),
        "553f88c9e4d10fc9e109e2aeb65f030801b70c2f6468faca261d401ae622fc87",
        "5da7c1a2acd57cee7505fc6676e4e544621c30862966e37dddb68e92efbe5d6b",
    ),
    (
        "post-vanilla-query",
        SigV4Request(
            method="POST",
            path="/",
            query="Param1=value1",
            headers={"Host": "example.amazonaws.com", "X-Amz-Date": AMZ_DATE},
        ),
        "\n".join(
            [
                "POST",
                "/",
                "Param1=value1",
                "host:example.amazonaws.com",
                f"x-amz-date:{AMZ_DATE}",
                "",
                "host;x-amz-date",
                _EMPTY_BODY_HASH,
            ]
        ),
        "9d659678c1756bb3113e2ce898845a0a79dbbc57b740555917687f1b3340fbbd",
        "28038455d6de14eafc1f9222cf5aa6f1a96197d7deb8263271d420d138af7f11",
    ),
    (
        "post-x-www-form-urlencoded",
        SigV4Request(
            method="POST",
            path="/",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Host": "example.amazonaws.com",
                "X-Amz-Date": AMZ_DATE,
            },
            payload=b"Param1=value1",
        ),
        "\n".join(
            [
                "POST",
                "/",
                "",
                "content-type:application/x-www-form-urlencoded",
                "host:example.amazonaws.com",
                f"x-amz-date:{AMZ_DATE}",
                "",
                "content-type;host;x-amz-date",
                "9095672bbd1f56dfc5b65f3e153adc8731a4a654192329106275f4c7b24d0b6e",
            ]
        ),
        "42a5e5bb34198acb3e84da4f085bb7927f2bc277ca766e6d19c73c2154021281",
        "ff11897932ad3f4e8b18135d722051e5ac45fc38421b1da7b9d196a0fe09473a",
    ),
    (
        "get-header-value-trim",
        SigV4Request(
            method="GET",
            path="/",
            headers={
                "Host": "example.amazonaws.com",
                "My-Header1": "value1",
                "My-Header2": '"a   b   c"',
                "X-Amz-Date": AMZ_DATE,
            },
        ),
        "\n".join(
            [
                "GET",
                "/",
                "",
                "host:example.amazonaws.com",
                "my-header1:value1",
                'my-header2:"a b c"',
                f"x-amz-date:{AMZ_DATE}",
                "",
                "host;my-header1;my-header2;x-amz-date",
                _EMPTY_BODY_HASH,
            ]
        ),
        "a726db9b0df21c14f559d0a978e563112acb1b9e05476f0a6a1c7d68f28605c7",
        "acc3ed3afb60bb290fc8d2dd0098b9911fcaa05412b367055dee359757a9c736",
    ),
]


@pytest.mark.parametrize(
    ("name", "request_", "expected_creq", "expected_sts_hash", "expected_signature"),
    VECTORS,
    ids=[v[0] for v in VECTORS],
)
def test_vector(
    name: str,
    request_: SigV4Request,
    expected_creq: str,
    expected_sts_hash: str,
    expected_signature: str,
) -> None:
    creq, _signed_headers = canonical_request(request_)
    assert creq == expected_creq

    scope = credential_scope(DATE_STAMP, REGION, SERVICE)
    sts = string_to_sign(AMZ_DATE, scope, creq)
    assert sts.splitlines()[-1] == expected_sts_hash
    assert sts == f"AWS4-HMAC-SHA256\n{AMZ_DATE}\n{scope}\n{expected_sts_hash}"

    key = derive_signing_key(SECRET_ACCESS_KEY, DATE_STAMP, REGION, SERVICE)
    assert sign_string(key, sts) == expected_signature


def test_credential_scope_format() -> None:
    assert (
        credential_scope(DATE_STAMP, REGION, SERVICE) == "20150830/us-east-1/service/aws4_request"
    )


def test_empty_path_is_root() -> None:
    creq, _ = canonical_request(SigV4Request(method="GET", path=""))
    assert creq.splitlines()[1] == "/"


def test_signed_headers_are_sorted_and_lowercased() -> None:
    req = SigV4Request(
        method="GET",
        path="/",
        headers={"X-Amz-Date": AMZ_DATE, "Host": "example.amazonaws.com", "Zebra": "z"},
    )
    _, signed_headers = canonical_request(req)
    assert signed_headers == "host;x-amz-date;zebra"
