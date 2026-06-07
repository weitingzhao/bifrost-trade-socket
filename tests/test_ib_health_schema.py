"""Tests for canonical IB Broker Redis health field helpers."""

from __future__ import annotations

from bifrost_socket.ib.account_agent.redis_writer import IbAccountAgentRedisWriter
from bifrost_socket.ib.ib_health_schema import (
    HOST_PROBE_AT,
    HOST_PROBE_INTERVAL,
    HOST_PROBE_OK,
    SECONDARY_PROBE_AT,
    ingestor_host_mirror_fields,
    slot_probe_fields,
)
from bifrost_socket.ib.ingestor.redis_writer import IbIngestorRedisWriter


class _FakeHealthWriter:
    def __init__(self) -> None:
        self.last_fields: dict | None = None

    def write(self, fields: dict) -> None:
        self.last_fields = fields


class _FakeRds:
    def __init__(self) -> None:
        self.health = _FakeHealthWriter()

    def delete(self, _key: str) -> None:
        pass


def test_slot_probe_fields_host_canonical_names() -> None:
    fields = slot_probe_fields("host", probe_at=100.0, probe_ok=True, probe_interval_sec=15.0)
    assert fields[HOST_PROBE_AT] == 100.0
    assert fields[HOST_PROBE_OK] is True
    assert fields[HOST_PROBE_INTERVAL] == 15.0


def test_ingestor_host_mirror_matches_top_level_probe() -> None:
    mirror = ingestor_host_mirror_fields(
        client_id=50,
        connected=True,
        probe_at=200.0,
        probe_ok=True,
        probe_interval_sec=15.0,
    )
    assert mirror["host_client_id"] == 50
    assert mirror["host_connected"] is True
    assert mirror[HOST_PROBE_AT] == 200.0


def test_account_agent_writer_uses_canonical_probe_keys(monkeypatch) -> None:
    rds = _FakeRds()
    writer = IbAccountAgentRedisWriter(rds)
    writer._health = rds.health  # type: ignore[method-assign]
    writer.write_health(
        host_client_id=60,
        host_connected=True,
        last_msg_ts=1.0,
        reconnects=1,
        msg_count=5,
        secondary_connected=True,
        secondary_client_id=61,
        host_probe_at=10.0,
        host_probe_ok=True,
        host_probe_interval_sec=15.0,
        secondary_probe_at=11.0,
        secondary_probe_ok=True,
        secondary_probe_interval_sec=15.0,
    )
    assert rds.health.last_fields is not None
    assert rds.health.last_fields[HOST_PROBE_AT] == 10.0
    assert rds.health.last_fields[SECONDARY_PROBE_AT] == 11.0
    assert "host_probe_at" not in rds.health.last_fields
    assert rds.health.last_fields["host_last_error"] == ""


def test_ingestor_writer_writes_host_mirror(monkeypatch) -> None:
    rds = _FakeRds()
    writer = IbIngestorRedisWriter(rds)
    writer._health = rds.health  # type: ignore[method-assign]
    writer.write_health(
        client_id=50,
        connected=True,
        last_msg_ts=1.0,
        reconnects=2,
        msg_count=10,
        ib_probe_at=5.0,
        ib_probe_ok=True,
        ib_probe_interval_sec=15.0,
    )
    assert rds.health.last_fields is not None
    assert rds.health.last_fields["ib_probe_at"] == 5.0
    assert rds.health.last_fields["host_ib_probe_at"] == 5.0
    assert rds.health.last_fields["host_client_id"] == 50
