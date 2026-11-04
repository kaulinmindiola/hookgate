import pytest

from hookgate.domain.errors import InvalidTransition
from hookgate.domain.states import (
    AttemptsAction,
    ColumnAction,
    EventStatus,
    TransitionResult,
    Trigger,
    transition,
)

S = EventStatus
T = Trigger
KEEP = AttemptsAction.KEEP
CLEAR = ColumnAction.CLEAR

LEGAL_PAIRS = {
    (S.RECEIVED, T.CLAIM),
    (S.FAILED, T.CLAIM),
    (S.PROCESSING, T.HANDLER_SUCCESS),
    (S.PROCESSING, T.PERMANENT_ERROR),
    (S.PROCESSING, T.TRANSIENT_ERROR),
    (S.PROCESSING, T.LEASE_EXPIRED),
    (S.DEAD, T.REPLAY),
}
ILLEGAL_PAIRS = [(s, t) for s in S for t in T if (s, t) not in LEGAL_PAIRS]


@pytest.mark.parametrize("status", [S.RECEIVED, S.FAILED])
def test_claim_increments_attempts_and_sets_the_lease(status: EventStatus) -> None:
    assert transition(status, T.CLAIM, attempts=1) == TransitionResult(
        from_status=status,
        to_status=S.PROCESSING,
        trigger=T.CLAIM,
        attempts=AttemptsAction.INCREMENT,
        locked_until=ColumnAction.SET,
        next_attempt_at=ColumnAction.KEEP,
    )


def test_handler_success_marks_processed_and_clears_schedule_and_lease() -> None:
    assert transition(S.PROCESSING, T.HANDLER_SUCCESS, attempts=1) == TransitionResult(
        S.PROCESSING, S.PROCESSED, T.HANDLER_SUCCESS, KEEP, CLEAR, CLEAR
    )


def test_permanent_error_goes_straight_to_dead() -> None:
    assert transition(S.PROCESSING, T.PERMANENT_ERROR, attempts=1) == TransitionResult(
        S.PROCESSING, S.DEAD, T.PERMANENT_ERROR, KEEP, CLEAR, CLEAR
    )


@pytest.mark.parametrize("trigger", [T.TRANSIENT_ERROR, T.LEASE_EXPIRED])
@pytest.mark.parametrize("attempts", [1, 2, 4])
def test_retryable_failure_below_max_attempts_goes_to_failed(
    trigger: Trigger, attempts: int
) -> None:
    assert transition(S.PROCESSING, trigger, attempts=attempts, max_attempts=5) == (
        TransitionResult(S.PROCESSING, S.FAILED, trigger, KEEP, CLEAR, ColumnAction.SET)
    )


@pytest.mark.parametrize("trigger", [T.TRANSIENT_ERROR, T.LEASE_EXPIRED])
@pytest.mark.parametrize("attempts", [5, 6])
def test_retryable_failure_at_or_above_max_attempts_goes_to_dead(
    trigger: Trigger, attempts: int
) -> None:
    assert transition(S.PROCESSING, trigger, attempts=attempts, max_attempts=5) == (
        TransitionResult(S.PROCESSING, S.DEAD, trigger, KEEP, CLEAR, CLEAR)
    )


def test_replay_resets_attempts_and_clears_schedule_and_lease() -> None:
    assert transition(S.DEAD, T.REPLAY, attempts=5) == TransitionResult(
        S.DEAD, S.RECEIVED, T.REPLAY, AttemptsAction.RESET, CLEAR, CLEAR
    )


def test_reason_is_the_trigger_value() -> None:
    assert transition(S.DEAD, T.REPLAY).reason == "operator_replay"


@pytest.mark.parametrize(("status", "trigger"), ILLEGAL_PAIRS)
def test_every_other_pair_is_rejected(status: EventStatus, trigger: Trigger) -> None:
    with pytest.raises(InvalidTransition):
        transition(status, trigger, attempts=1)


@pytest.mark.parametrize("trigger", list(Trigger))
def test_processed_is_terminal(trigger: Trigger) -> None:
    with pytest.raises(InvalidTransition):
        transition(S.PROCESSED, trigger)


def test_dead_must_pass_through_received() -> None:
    with pytest.raises(InvalidTransition) as exc_info:
        transition(S.DEAD, T.CLAIM)
    assert exc_info.value.status == "dead"
    assert exc_info.value.trigger == "claim"


def test_negative_attempts_are_rejected() -> None:
    with pytest.raises(ValueError, match="attempts"):
        transition(S.PROCESSING, T.TRANSIENT_ERROR, attempts=-1)


def test_max_attempts_below_one_is_rejected() -> None:
    with pytest.raises(ValueError, match="max_attempts"):
        transition(S.PROCESSING, T.TRANSIENT_ERROR, attempts=1, max_attempts=0)
