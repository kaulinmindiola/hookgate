"""VR-4, real: a worker process killed mid-task (FR-PRO-2/6/7, NFR-1).

The victim is a real Celery worker process that claims the event and hangs inside the
handler. It is killed with no chance to clean up (SIGKILL; TerminateProcess on
Windows), holding the claim and an unfinished attempt row. With ``LEASE_S=2`` the
sweeper recovers the event and a second worker applies the effect, exactly once.
"""

import pytest
from tests.chaos.services import wait_until
from tests.chaos.support import Factory, attempts, count, insert_event, load
from tests.chaos.workers import StartWorker
from tests.fakes import FixedRng

from hookgate.application.sweep import SweepEvents
from hookgate.domain.retry import RetryPolicy
from hookgate.domain.states import EventStatus
from hookgate.infrastructure.db.engine import session_scope
from hookgate.infrastructure.db.payments import PaymentProjection
from hookgate.infrastructure.db.repositories import EventRepository
from hookgate.infrastructure.db.transactions import sweep_scope
from hookgate.infrastructure.queue import CeleryPublisher

pytestmark = pytest.mark.chaos

S = EventStatus


def sweeper(factory: Factory, publisher: CeleryPublisher) -> SweepEvents:
    return SweepEvents(
        units=sweep_scope(factory),
        publisher=publisher,
        policy=RetryPolicy(max_attempts=5, base_s=0.2, cap_s=0.5),
        rng=FixedRng(1.0),
        received_grace_s=1,
    )


def test_killed_worker_leaves_a_claim_that_the_sweeper_recovers_and_applies_once(
    session_factory: Factory, start_worker: StartWorker, publisher: CeleryPublisher
) -> None:
    event_id = insert_event(session_factory, "evt_kill_1")
    victim = start_worker(block_prefix="evt_kill")

    publisher.publish(event_id, trace_id="trace-kill")
    wait_until(lambda: victim.claimed("evt_kill_1"), timeout_s=30, what="the victim to claim")
    claimed = load(session_factory, event_id)
    assert (claimed.status, claimed.attempts) == (S.PROCESSING, 1)

    victim.kill()

    assert victim.process.returncode != 0
    (open_attempt,) = attempts(session_factory, event_id)
    assert open_attempt.finished_at is None  # evidence of the crash
    assert load(session_factory, event_id).status is S.PROCESSING

    start_worker()  # a healthy worker; the broker may also redeliver the unacked message
    sweep = sweeper(session_factory, publisher)

    def recovered() -> bool:
        sweep()
        return load(session_factory, event_id).status is S.PROCESSED

    wait_until(recovered, timeout_s=60, poll_s=0.5, what="the event to be recovered")

    final = load(session_factory, event_id)
    history = [(a.attempt_no, a.result) for a in attempts(session_factory, event_id)]
    with session_scope(session_factory) as session:
        reasons = [t.reason for t in EventRepository(session).list_transitions(event_id)]
        payment = PaymentProjection(session).get("pay_evt_kill_1")
        effects = PaymentProjection(session).effect_count(event_id)
    assert final.attempts == 2
    assert history == [(1, "lease_expired"), (2, "processed")]
    assert reasons == ["ingested", "claim", "lease_expired", "claim", "handler_success"]
    assert effects == 1
    assert payment is not None
    assert payment.last_event_id == "evt_kill_1"


def test_no_event_is_lost_when_a_busy_worker_is_killed(
    session_factory: Factory, start_worker: StartWorker, publisher: CeleryPublisher
) -> None:
    """NFR-1: twenty events in flight, one of them hung; the worker dies; all end once."""
    ids = [insert_event(session_factory, "evt_kill_hang")]
    ids += [insert_event(session_factory, f"evt_ok_{n:02d}") for n in range(20)]
    victim = start_worker(block_prefix="evt_kill")
    for event_id in ids:
        publisher.publish(event_id, trace_id="t")
    wait_until(lambda: victim.claimed("evt_kill_hang"), timeout_s=30, what="the hang")

    victim.kill()
    start_worker()
    sweep = sweeper(session_factory, publisher)

    def all_processed() -> bool:
        sweep()
        return all(load(session_factory, i).status is S.PROCESSED for i in ids)

    wait_until(all_processed, timeout_s=90, poll_s=0.5, what="every event to be processed")
    assert count(session_factory, "applied_effects") == len(ids)
    assert count(session_factory, "payments") == len(ids)
