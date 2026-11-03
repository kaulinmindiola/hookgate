"""Webhook signature and timestamp verification (FR-ING-1..3, SDD section 10).

Header:  ``Hookgate-Signature: t=<unix_seconds>,v1=<hex hmac-sha256>``
Signed payload: ``<t>.<raw body bytes>``. The comparison is constant-time.
"""

import hashlib
import hmac
import re
from dataclasses import dataclass
from datetime import datetime

from hookgate.domain.errors import SignatureError, SignatureFailure
from hookgate.domain.timeutil import require_aware

DEFAULT_TOLERANCE_S = 300

# No leading zeros, so the signed text and the integer always agree.
_TIMESTAMP = re.compile(r"0|[1-9][0-9]{0,11}")
_SIGNATURE = re.compile(r"[0-9a-fA-F]{64}")


@dataclass(frozen=True, slots=True)
class ParsedSignature:
    timestamp: int
    signature: str  # lowercase hex


def compute_signature(secret: str, timestamp: int, raw_body: bytes) -> str:
    payload = f"{timestamp}.".encode() + raw_body
    return hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()


def build_signature_header(secret: str, timestamp: int, raw_body: bytes) -> str:
    """Build the header value (used by the simulator and by tests)."""
    return f"t={timestamp},v1={compute_signature(secret, timestamp, raw_body)}"


def parse_signature_header(header: str | None) -> ParsedSignature:
    if header is None or not header.strip():
        raise SignatureError(SignatureFailure.MISSING)

    timestamps: list[str] = []
    signatures: list[str] = []
    for part in header.split(","):
        key, separator, value = part.strip().partition("=")
        if not separator:
            raise SignatureError(SignatureFailure.MALFORMED)
        if key == "t":
            timestamps.append(value)
        elif key == "v1":
            signatures.append(value)

    if len(timestamps) != 1 or len(signatures) != 1:
        raise SignatureError(SignatureFailure.MALFORMED)
    timestamp, signature = timestamps[0], signatures[0]
    if _TIMESTAMP.fullmatch(timestamp) is None or _SIGNATURE.fullmatch(signature) is None:
        raise SignatureError(SignatureFailure.MALFORMED)
    return ParsedSignature(timestamp=int(timestamp), signature=signature.lower())


def verify_signature(
    header: str | None,
    raw_body: bytes,
    secret: str,
    *,
    now: datetime,
    tolerance_s: int = DEFAULT_TOLERANCE_S,
) -> None:
    """Raise ``SignatureError`` unless the signature and the timestamp are valid.

    The signature is checked first, so the tolerance window is not revealed to a
    caller who does not know the secret.
    """
    require_aware(now, "now")
    if tolerance_s < 0:
        raise ValueError("tolerance_s must be >= 0")

    parsed = parse_signature_header(header)
    expected = compute_signature(secret, parsed.timestamp, raw_body)
    if not hmac.compare_digest(expected.encode("ascii"), parsed.signature.encode("ascii")):
        raise SignatureError(SignatureFailure.INVALID)
    if abs(now.timestamp() - parsed.timestamp) > tolerance_s:
        raise SignatureError(SignatureFailure.TIMESTAMP_OUT_OF_TOLERANCE)
