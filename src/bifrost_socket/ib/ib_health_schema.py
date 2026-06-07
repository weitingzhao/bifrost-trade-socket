"""Canonical Redis health hash field names for IB Broker socket services."""

from __future__ import annotations

from typing import Any, Dict, Literal, Optional

SlotRole = Literal["host", "secondary", "ingestor"]

# Ingestor top-level (legacy + mirror source).
INGESTOR_PROBE_AT = "ib_probe_at"
INGESTOR_PROBE_OK = "ib_probe_ok"
INGESTOR_PROBE_INTERVAL = "ib_probe_interval_sec"

# Per-slot probe fields (Operator, Account Agent, Ingestor host mirror).
HOST_PROBE_AT = "host_ib_probe_at"
HOST_PROBE_OK = "host_ib_probe_ok"
HOST_PROBE_INTERVAL = "host_ib_probe_interval_sec"
SECONDARY_PROBE_AT = "secondary_ib_probe_at"
SECONDARY_PROBE_OK = "secondary_ib_probe_ok"
SECONDARY_PROBE_INTERVAL = "secondary_ib_probe_interval_sec"

# Service heartbeat fields (shared).
SERVICE_HEARTBEAT_INTERVAL = "service_heartbeat_interval_sec"
LAST_SERVICE_HEARTBEAT_AT = "last_service_heartbeat_at"
NEXT_SERVICE_HEARTBEAT_IN_S = "next_service_heartbeat_in_s"
SERVICE_HEARTBEAT_RECONNECT = "service_heartbeat_reconnect_in_progress"


def _connected_str(connected: bool) -> str:
    return "1" if connected else "0"


def slot_probe_fields(
    role: SlotRole,
    *,
    probe_at: float,
    probe_ok: bool,
    probe_interval_sec: float,
) -> Dict[str, Any]:
    """Build canonical probe Redis fields for host, secondary, or ingestor top-level."""
    if role == "ingestor":
        return {
            INGESTOR_PROBE_AT: probe_at,
            INGESTOR_PROBE_OK: probe_ok,
            INGESTOR_PROBE_INTERVAL: probe_interval_sec,
        }
    prefix = "host" if role == "host" else "secondary"
    return {
        f"{prefix}_ib_probe_at": probe_at,
        f"{prefix}_ib_probe_ok": probe_ok,
        f"{prefix}_ib_probe_interval_sec": probe_interval_sec,
    }


def ingestor_host_mirror_fields(
    *,
    client_id: int,
    connected: bool,
    probe_at: float,
    probe_ok: bool,
    probe_interval_sec: float,
    last_error: Optional[str] = None,
) -> Dict[str, Any]:
    """Mirror ingestor top-level IB state under host_* for isomorphic Redis hashes."""
    out: Dict[str, Any] = {
        "host_connected": connected,
        "host_client_id": client_id,
        "host_reconnects": 0,  # filled by caller if needed
        **slot_probe_fields(
            "host",
            probe_at=probe_at,
            probe_ok=probe_ok,
            probe_interval_sec=probe_interval_sec,
        ),
    }
    if last_error is not None:
        out["host_last_error"] = last_error
    return out


def service_heartbeat_fields(
    *,
    interval_sec: float,
    last_heartbeat_at: float,
    next_in_s: float,
    reconnect_in_progress: str = "",
) -> Dict[str, Any]:
    """Build service heartbeat Redis fields when interval is configured."""
    if interval_sec <= 0:
        return {}
    return {
        SERVICE_HEARTBEAT_INTERVAL: interval_sec,
        LAST_SERVICE_HEARTBEAT_AT: last_heartbeat_at,
        NEXT_SERVICE_HEARTBEAT_IN_S: next_in_s,
        SERVICE_HEARTBEAT_RECONNECT: reconnect_in_progress,
    }
