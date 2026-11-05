"""HTTP clients: the provider side (signed deliveries) and the admin side (verify).

Signing is implemented here, independently of Hookgate: ``Hookgate-Signature:
t=<unix seconds>,v1=<hex HMAC-SHA256 of "<t>." + raw body>``. The timestamp is the
moment of *delivery*, not of creation, so a delayed event is signed when it is sent.
"""

import hashlib
import hmac
import json
import time
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from typing import Any

import httpx

from simulator.model import CURRENCY, Delivery, EventSpec, Signing
from simulator.oracle import ObservedEvent, PaymentState, Response

WEBHOOK_PATH = "/webhooks/payments"
SIGNATURE_HEADER = "Hookgate-Signature"
EXPIRED_BY_S = 3600  # far outside any reasonable tolerance window
PAGE_SIZE = 200


def envelope(event: EventSpec) -> bytes:
    document = {
        "id": event.event_id,
        "type": event.event_type,
        "occurred_at": event.occurred_at.isoformat().replace("+00:00", "Z"),
        "resource": {"type": "payment", "id": event.resource_id},
        "data": {"amount": event.amount, "currency": CURRENCY},
    }
    return json.dumps(document, separators=(",", ":")).encode()


def signature_header(secret: str, timestamp: int, body: bytes) -> str:
    mac = hmac.new(secret.encode(), f"{timestamp}.".encode() + body, hashlib.sha256)
    return f"t={timestamp},v1={mac.hexdigest()}"


class ProviderClient:
    def __init__(
        self, http: httpx.Client, *, secret: str, now: Callable[[], float] | None = None
    ) -> None:
        self._http = http
        self._secret = secret
        self._now = now or time.time

    def deliver(self, delivery: Delivery) -> list[Response]:
        if not delivery.concurrent or delivery.copies == 1:
            return [self._send(delivery) for _ in range(delivery.copies)]
        barrier = Barrier(delivery.copies)

        def at_once(_: int) -> Response:
            barrier.wait(timeout=30)
            return self._send(delivery)

        with ThreadPoolExecutor(max_workers=delivery.copies) as pool:
            return list(pool.map(at_once, range(delivery.copies)))

    def _send(self, delivery: Delivery) -> Response:
        body = envelope(delivery.event)
        timestamp = int(self._now())
        secret = self._secret
        if delivery.signing is Signing.BAD_SIGNATURE:
            secret = f"{self._secret}-not-the-shared-secret"
        elif delivery.signing is Signing.EXPIRED:
            timestamp -= EXPIRED_BY_S
        response = self._http.post(
            WEBHOOK_PATH,
            content=body,
            headers={
                "Content-Type": "application/json",
                SIGNATURE_HEADER: signature_header(secret, timestamp, body),
                "X-Trace-Id": f"sim-{delivery.event.event_id}",
            },
        )
        return Response(delivery.event.event_id, response.status_code, _result(response))


def _result(response: httpx.Response) -> str:
    try:
        payload: Any = response.json()
    except ValueError:
        return response.text[:100]
    if isinstance(payload, dict):
        return str(payload.get("status") or payload.get("error") or payload)
    return str(payload)


class AdminClient:
    def __init__(self, http: httpx.Client, *, api_key: str) -> None:
        self._http = http
        self._headers = {"X-API-Key": api_key}

    def events(self, *, prefix: str, status: str | None = None) -> dict[str, ObservedEvent]:
        """Every stored event whose id starts with ``prefix`` (walks all pages)."""
        found: dict[str, ObservedEvent] = {}
        for item in self._pages(status):
            event_id = str(item["event_id"])
            if event_id.startswith(prefix):
                found[event_id] = ObservedEvent(
                    event_id=event_id,
                    status=str(item["status"]),
                    outcome=None if item["outcome"] is None else str(item["outcome"]),
                )
        return found

    def payment(self, resource_id: str) -> PaymentState | None:
        response = self._http.get(f"/admin/payments/{resource_id}", headers=self._headers)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        body: dict[str, Any] = response.json()
        return PaymentState(
            status=body["status"],
            amount=body["amount"],
            currency=body["currency"],
            last_event_id=body["last_event_id"],
        )

    def _pages(self, status: str | None) -> Iterator[dict[str, Any]]:
        params: dict[str, Any] = {"limit": PAGE_SIZE}
        if status is not None:
            params["status"] = status
        while True:
            response = self._http.get("/admin/events", params=params, headers=self._headers)
            response.raise_for_status()
            page: dict[str, Any] = response.json()
            yield from page["items"]
            if page["next_cursor"] is None:
                return
            params["cursor"] = page["next_cursor"]
