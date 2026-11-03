import json

import pytest
import structlog

from hookgate.infrastructure.logging import configure_logging


def test_log_lines_are_json_with_level_timestamp_and_bound_fields(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging()

    structlog.get_logger("test").bind(trace_id="t-1", event_id="evt_1").info("webhook_accepted")

    line = capsys.readouterr().out.strip().splitlines()[-1]
    entry = json.loads(line)
    assert entry["event"] == "webhook_accepted"
    assert entry["trace_id"] == "t-1"
    assert entry["event_id"] == "evt_1"
    assert entry["level"] == "info"
    assert entry["timestamp"].endswith("Z")
