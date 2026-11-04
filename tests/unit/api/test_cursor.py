"""Opaque admin page cursors (FR-DLQ-1)."""

import base64
from datetime import UTC, datetime, timedelta, timezone
from uuid import uuid4

import pytest

from hookgate.api.cursor import InvalidCursor, decode_cursor, encode_cursor
from hookgate.application.records import PageKey


@pytest.mark.parametrize(
    "received_at",
    [
        datetime(2026, 11, 3, 12, 0, 0, tzinfo=UTC),
        datetime(2026, 11, 3, 12, 0, 0, 123456, tzinfo=UTC),
        datetime(2026, 11, 3, 7, 0, 0, 1, tzinfo=timezone(timedelta(hours=-5))),
    ],
)
def test_round_trip_keeps_the_exact_position(received_at: datetime) -> None:
    key = PageKey(received_at=received_at, id=uuid4())

    decoded = decode_cursor(encode_cursor(key))

    assert decoded == key
    assert decoded.received_at.utcoffset() == received_at.utcoffset()


def test_cursor_is_url_safe_without_padding() -> None:
    cursor = encode_cursor(PageKey(received_at=datetime.now(UTC), id=uuid4()))

    assert "=" not in cursor
    assert set(cursor) <= set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_")


def _b64(text: str) -> str:
    return base64.urlsafe_b64encode(text.encode()).decode().rstrip("=")


@pytest.mark.parametrize(
    "cursor",
    [
        "",
        "not base64 !",
        "ñ",
        _b64("no-separator"),
        _b64("2026-11-03T12:00:00+00:00|not-a-uuid"),
        _b64(f"yesterday|{uuid4()}"),
        _b64(f"2026-11-03T12:00:00|{uuid4()}"),  # naive datetime
        _b64(f"2026-11-03T12:00:00+00:00|{uuid4()}|extra"),
        base64.urlsafe_b64encode(b"\xff\xfe").decode(),
    ],
)
def test_foreign_or_altered_cursors_are_rejected(cursor: str) -> None:
    with pytest.raises(InvalidCursor):
        decode_cursor(cursor)
