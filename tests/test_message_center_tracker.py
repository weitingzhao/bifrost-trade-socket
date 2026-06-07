"""Unit tests for IB connection status → message center publishing."""

from __future__ import annotations

from bifrost_core.core.message_center import IbConnectionStatusTracker
from bifrost_socket.ib.connection_lifecycle import REASON_SERVICE_STOPPED, publish_operator_slots


class FakeRedis:
    def __init__(self) -> None:
        self.stream: list[tuple[str, dict[str, str]]] = []

    def xadd(self, key: str, fields: dict[str, str], maxlen=None, approximate=None) -> str:
        entry_id = f"{len(self.stream) + 1}-0"
        self.stream.append((entry_id, {"_stream_key": key, **fields}))
        return entry_id


def test_status_tracker_skips_initial_disconnected_but_publishes_first_connected() -> None:
    redis = FakeRedis()
    tracker = IbConnectionStatusTracker(redis, service="ib_ingestor")

    assert tracker.update(slot="host", status="disconnected", client_id=150) is None
    assert redis.stream == []

    tracker.update(slot="host", status="connected", client_id=150, occurred_at=100.0)
    assert len(redis.stream) == 1
    _entry_id, payload = redis.stream[0]
    assert payload["status_from"] == "disconnected"
    assert payload["status_to"] == "connected"
    assert payload["service"] == "ib_ingestor"
    assert payload["slot"] == "host"


def test_publish_operator_slots_emits_host_and_secondary() -> None:
    redis = FakeRedis()
    tracker = IbConnectionStatusTracker(redis, service="ib_operator")

    tracker.update(slot="host", status="disconnected", client_id=20)
    tracker.update(slot="secondary", status="disconnected", client_id=21)
    assert redis.stream == []

    publish_operator_slots(
        tracker,
        {
            "updated_at": 200.0,
            "host": {"connected": True, "client_id": 20, "last_error": ""},
            "secondary": {"connected": True, "client_id": 21, "last_error": ""},
        },
    )
    assert len(redis.stream) == 2
    slots = {p["slot"] for _, p in redis.stream}
    assert slots == {"host", "secondary"}
    for _eid, payload in redis.stream:
        assert payload["status_to"] == "connected"
        assert payload["service"] == "ib_operator"
        assert payload["topic"] == "ib.connection"


def test_publish_operator_slots_service_stopped_reason() -> None:
    redis = FakeRedis()
    tracker = IbConnectionStatusTracker(redis, service="ib_operator")
    tracker.update(slot="host", status="connected", client_id=20)
    tracker.update(slot="secondary", status="connected", client_id=21)
    redis.stream.clear()

    publish_operator_slots(
        tracker,
        {
            "updated_at": 300.0,
            "host": {"connected": False, "client_id": 20, "last_error": ""},
            "secondary": {"connected": False, "client_id": 21, "last_error": ""},
        },
        reason=REASON_SERVICE_STOPPED,
    )
    assert len(redis.stream) == 2
    for _eid, payload in redis.stream:
        assert payload["status_to"] == "disconnected"
        assert payload["reason"] == REASON_SERVICE_STOPPED
