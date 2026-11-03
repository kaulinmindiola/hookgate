"""Small helpers shared by domain modules."""

from datetime import datetime


def require_aware(value: datetime, name: str) -> None:
    """Reject naive datetimes: every instant in the domain is timezone-aware."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
