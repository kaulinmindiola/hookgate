"""Error taxonomy (SDD section 12) and domain-level exceptions."""

from enum import StrEnum


class HookgateError(Exception):
    """Base class for every domain error."""


class TransientError(HookgateError):
    """A failure that may succeed on retry (timeouts, connection errors, deadlocks)."""


class PermanentError(HookgateError):
    """A failure that retrying cannot fix (schema violation, unknown event type)."""


class InvalidTransition(HookgateError):
    """The requested trigger is not allowed from the current status (SDD section 9)."""

    def __init__(self, status: str, trigger: str) -> None:
        super().__init__(f"trigger '{trigger}' is not allowed from status '{status}'")
        self.status = status
        self.trigger = trigger


class SignatureFailure(StrEnum):
    """Machine-readable reasons for rejecting a webhook signature (FR-ING-2, FR-ING-3)."""

    MISSING = "missing_signature"
    MALFORMED = "malformed_signature"
    INVALID = "invalid_signature"
    TIMESTAMP_OUT_OF_TOLERANCE = "timestamp_out_of_tolerance"


class SignatureError(HookgateError):
    """The webhook signature or its timestamp was rejected."""

    def __init__(self, reason: SignatureFailure) -> None:
        super().__init__(reason.value)
        self.reason = reason
