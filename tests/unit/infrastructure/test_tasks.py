"""Reading the trace id from a Celery request (FR-OBS-1)."""

from typing import Any

import pytest

from hookgate.infrastructure.tasks import message_trace_id


class FakeRequest(dict[str, Any]):
    """Celery's ``Context`` behaves like this: ``get`` plus a ``headers`` attribute."""

    def __init__(self, headers: dict[str, Any] | None = None, **attributes: Any) -> None:
        super().__init__(**attributes)
        self.headers = headers


@pytest.mark.parametrize(
    ("request_", "expected"),
    [
        (FakeRequest(trace_id="from-broker"), "from-broker"),
        (FakeRequest(headers={"trace_id": "eager"}), "eager"),
        (FakeRequest(headers={"trace_id": "h"}, trace_id="attr"), "attr"),
        (FakeRequest(), None),
        (FakeRequest(headers={}, trace_id=""), None),
    ],
)
def test_trace_id_comes_from_the_message_header(
    request_: FakeRequest, expected: str | None
) -> None:
    assert message_trace_id(request_) == expected
