"""Opaque page cursors for the admin list (FR-DLQ-1).

A cursor is the URL-safe base64 of ``<received_at ISO 8601>|<id>`` without padding.
Clients must treat it as opaque; the format can change without notice.
"""

import base64
import binascii
from datetime import datetime
from uuid import UUID

from hookgate.application.records import PageKey

_SEPARATOR = "|"


class InvalidCursor(Exception):
    """The cursor was not produced by this API (or was altered)."""


def encode_cursor(key: PageKey) -> str:
    raw = f"{key.received_at.isoformat()}{_SEPARATOR}{key.id}".encode()
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def decode_cursor(cursor: str) -> PageKey:
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        raw = base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
        received_at_text, id_text = raw.split(_SEPARATOR)
        received_at = datetime.fromisoformat(received_at_text)
        return PageKey(received_at=received_at, id=UUID(id_text))
    except (ValueError, UnicodeError, binascii.Error) as exc:
        raise InvalidCursor(cursor) from exc
