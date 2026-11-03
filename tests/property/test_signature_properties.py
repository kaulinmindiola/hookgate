"""VR-8 (property part): tampering, wrong secret and skew edges."""

from datetime import UTC, datetime

import pytest
from hypothesis import assume, given
from hypothesis import strategies as st

from hookgate.domain.errors import SignatureError, SignatureFailure
from hookgate.domain.signature import build_signature_header, verify_signature

TOLERANCE = 300

secrets = st.text(min_size=1, max_size=40)
bodies = st.binary(max_size=512)
timestamps = st.integers(min_value=0, max_value=4_000_000_000)


def at(timestamp: int) -> datetime:
    return datetime.fromtimestamp(timestamp, tz=UTC)


@given(secret=secrets, body=bodies, timestamp=timestamps)
def test_a_correctly_signed_body_always_verifies(secret: str, body: bytes, timestamp: int) -> None:
    header = build_signature_header(secret, timestamp, body)
    verify_signature(header, body, secret, now=at(timestamp), tolerance_s=TOLERANCE)


@given(
    secret=secrets,
    body=st.binary(min_size=1, max_size=512),
    timestamp=timestamps,
    index=st.integers(min_value=0),
    flip=st.integers(min_value=1, max_value=255),
)
def test_changing_any_byte_of_the_body_invalidates_the_signature(
    secret: str, body: bytes, timestamp: int, index: int, flip: int
) -> None:
    header = build_signature_header(secret, timestamp, body)
    tampered = bytearray(body)
    tampered[index % len(tampered)] ^= flip
    with pytest.raises(SignatureError) as exc_info:
        verify_signature(header, bytes(tampered), secret, now=at(timestamp))
    assert exc_info.value.reason == SignatureFailure.INVALID


@given(secret=secrets, other=secrets, body=bodies, timestamp=timestamps)
def test_a_different_secret_never_verifies(
    secret: str, other: str, body: bytes, timestamp: int
) -> None:
    assume(secret != other)
    header = build_signature_header(secret, timestamp, body)
    with pytest.raises(SignatureError) as exc_info:
        verify_signature(header, body, other, now=at(timestamp))
    assert exc_info.value.reason == SignatureFailure.INVALID


@given(
    secret=secrets,
    body=bodies,
    now_ts=st.integers(min_value=2_000, max_value=4_000_000_000),
    skew=st.integers(min_value=-1_000, max_value=1_000),
)
def test_acceptance_is_decided_exactly_by_the_tolerance(
    secret: str, body: bytes, now_ts: int, skew: int
) -> None:
    header = build_signature_header(secret, now_ts + skew, body)
    if abs(skew) <= TOLERANCE:
        verify_signature(header, body, secret, now=at(now_ts), tolerance_s=TOLERANCE)
    else:
        with pytest.raises(SignatureError) as exc_info:
            verify_signature(header, body, secret, now=at(now_ts), tolerance_s=TOLERANCE)
        assert exc_info.value.reason == SignatureFailure.TIMESTAMP_OUT_OF_TOLERANCE
