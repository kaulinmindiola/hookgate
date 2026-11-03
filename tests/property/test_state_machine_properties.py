"""VR-12: random trigger sequences never reach an illegal transition."""

from hypothesis import settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, rule

from hookgate.domain.errors import InvalidTransition
from hookgate.domain.states import AttemptsAction, EventStatus, Trigger, transition

S = EventStatus
T = Trigger
MAX_ATTEMPTS = 3

# Independent oracle written from SDD section 9, not from the implementation.
LEGAL: dict[tuple[EventStatus, Trigger], set[EventStatus]] = {
    (S.RECEIVED, T.CLAIM): {S.PROCESSING},
    (S.FAILED, T.CLAIM): {S.PROCESSING},
    (S.PROCESSING, T.HANDLER_SUCCESS): {S.PROCESSED},
    (S.PROCESSING, T.PERMANENT_ERROR): {S.DEAD},
    (S.PROCESSING, T.TRANSIENT_ERROR): {S.FAILED, S.DEAD},
    (S.PROCESSING, T.LEASE_EXPIRED): {S.FAILED, S.DEAD},
    (S.DEAD, T.REPLAY): {S.RECEIVED},
}


@settings(max_examples=200, stateful_step_count=40, deadline=None)
class EventLifecycleMachine(RuleBasedStateMachine):
    def __init__(self) -> None:
        super().__init__()
        self.status = S.RECEIVED
        self.attempts = 0
        self.reached_processed = False

    @rule(trigger=st.sampled_from(Trigger))
    def fire(self, trigger: Trigger) -> None:
        try:
            result = transition(
                self.status, trigger, attempts=self.attempts, max_attempts=MAX_ATTEMPTS
            )
        except InvalidTransition:
            assert (self.status, trigger) not in LEGAL
            return

        assert (self.status, trigger) in LEGAL
        assert result.from_status is self.status
        assert result.to_status in LEGAL[(self.status, trigger)]

        if result.to_status is S.FAILED:
            assert self.attempts < MAX_ATTEMPTS
        if result.to_status is S.DEAD and trigger in (T.TRANSIENT_ERROR, T.LEASE_EXPIRED):
            assert self.attempts >= MAX_ATTEMPTS

        if result.attempts is AttemptsAction.INCREMENT:
            self.attempts += 1
        elif result.attempts is AttemptsAction.RESET:
            self.attempts = 0

        self.status = result.to_status
        if self.status is S.PROCESSED:
            self.reached_processed = True

    @invariant()
    def processed_is_terminal(self) -> None:
        if self.reached_processed:
            assert self.status is S.PROCESSED

    @invariant()
    def attempts_are_never_negative(self) -> None:
        assert self.attempts >= 0


TestEventLifecycle = EventLifecycleMachine.TestCase
