import hashlib
import hmac
from datetime import UTC, datetime

import pytest

from hookgate.domain.errors import SignatureError, SignatureFailure
from hookgate.domain.signature import (
    build_signature_header,
    compute_signature,
    parse_signature_header,
    verify_signature,
)

SECRET = "whsec_test"
BODY = b'{"id":"evt_1","type":"payment.created"}'
NOW = datetime(2026, 11, 2, 12, 0, 0, tzinfo=UTC)
NOW_TS = int(NOW.timestamp())
HEX = "a" * 64


def header_for(timestamp: int = NOW_TS, body: bytes = BODY, secret: str = SECRET) -> str:
    return build_signature_header(secret, timestamp, body)


def reason_of(header: str | None, body: bytes = BODY, **kwargs: int) -> SignatureFailure:
    with pytest.raises(SignatureError) as exc_info:
        verify_signature(header, body, SECRET, now=NOW, **kwargs)
    return exc_info.value.reason


def test_signed_payload_is_timestamp_dot_raw_body() -> None:
    expected = hmac.new(SECRET.encode(), b"1700000000." + BODY, hashlib.sha256).hexdigest()
    assert compute_signature(SECRET, 1_700_000_000, BODY) == expected


def test_valid_signature_is_accepted() -> None:
    verify_signature(header_for(), BODY, SECRET, now=NOW)


def test_uppercase_hex_is_accepted() -> None:
    timestamp, signature = header_for().split(",")
    uppercase_hex = signature.removeprefix("v1=").upper()
    verify_signature(f"{timestamp},v1={uppercase_hex}", BODY, SECRET, now=NOW)


def test_unknown_header_keys_are_ignored() -> None:
    verify_signature(f"{header_for()},v0=whatever", BODY, SECRET, now=NOW)


@pytest.mark.parametrize("header", [None, "", "   "])
def test_missing_header(header: str | None) -> None:
    assert reason_of(header) == SignatureFailure.MISSING


@pytest.mark.parametrize(
    "header",
    [
        "garbage",
        f"t=abc,v1={HEX}",
        f"t=,v1={HEX}",
        f"t=-5,v1={HEX}",
        f"t=007,v1={HEX}",
        f"t=1700000000000000,v1={HEX}",
        "t=1700000000",
        f"v1={HEX}",
        "t=1700000000,v1=zz",
        f"t=1700000000,v1={'a' * 63}",
        f"t=1700000000,v1={'a' * 65}",
        f"t=1,t=2,v1={HEX}",
        f"t=1700000000,v1={HEX},v1={HEX}",
        f"t=1700000000,v1={HEX},oops",
    ],
)
def test_malformed_header(header: str) -> None:
    assert reason_of(header) == SignatureFailure.MALFORMED


def test_tampered_body_is_invalid() -> None:
    assert reason_of(header_for(), body=BODY + b" ") == SignatureFailure.INVALID


def test_wrong_secret_is_invalid() -> None:
    assert reason_of(header_for(secret="other")) == SignatureFailure.INVALID


def test_tampered_timestamp_is_invalid() -> None:
    _, signature = header_for().split(",")
    assert reason_of(f"t={NOW_TS + 1},{signature}") == SignatureFailure.INVALID


@pytest.mark.parametrize("skew", [-300, 0, 300])
def test_timestamp_inside_the_window_is_accepted(skew: int) -> None:
    verify_signature(header_for(NOW_TS + skew), BODY, SECRET, now=NOW)


@pytest.mark.parametrize("skew", [-301, 301])
def test_timestamp_outside_the_window_is_rejected(skew: int) -> None:
    reason = reason_of(header_for(NOW_TS + skew))
    assert reason == SignatureFailure.TIMESTAMP_OUT_OF_TOLERANCE


def test_tolerance_is_configurable() -> None:
    verify_signature(header_for(NOW_TS), BODY, SECRET, now=NOW, tolerance_s=0)
    assert (
        reason_of(header_for(NOW_TS + 1), tolerance_s=0)
        == SignatureFailure.TIMESTAMP_OUT_OF_TOLERANCE
    )


def test_invalid_signature_takes_precedence_over_an_expired_timestamp() -> None:
    assert reason_of(header_for(NOW_TS - 10_000, secret="other")) == SignatureFailure.INVALID


def test_naive_now_is_rejected() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        verify_signature(header_for(), BODY, SECRET, now=datetime(2026, 11, 2))


def test_negative_tolerance_is_rejected() -> None:
    with pytest.raises(ValueError, match="tolerance_s"):
        verify_signature(header_for(), BODY, SECRET, now=NOW, tolerance_s=-1)


def test_parse_returns_timestamp_and_lowercase_signature() -> None:
    parsed = parse_signature_header(f"t=1700000000,v1={HEX.upper()}")
    assert parsed.timestamp == 1_700_000_000
    assert parsed.signature == HEX
