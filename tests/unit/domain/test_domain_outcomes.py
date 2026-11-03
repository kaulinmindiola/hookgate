from hookgate.domain.outcomes import EventOutcome


def test_outcome_values_match_the_database_enum() -> None:
    assert {outcome.value for outcome in EventOutcome} == {"applied", "ignored_stale"}
