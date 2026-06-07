"""Deprecated: import from bifrost_socket.ib.connection_lifecycle instead."""

from __future__ import annotations

from bifrost_socket.ib.connection_lifecycle import (
    REASON_SERVICE_STOPPED,
    REASON_SESSION_ENDED,
    IbSlotSnapshot,
    publish_account_agent_slots,
    publish_ib_slots,
    publish_ingestor_slot,
    publish_operator_slots,
    slots_from_operator_health,
)

__all__ = [
    "REASON_SERVICE_STOPPED",
    "REASON_SESSION_ENDED",
    "IbSlotSnapshot",
    "publish_account_agent_slots",
    "publish_ib_slots",
    "publish_ingestor_slot",
    "publish_operator_slots",
    "slots_from_operator_health",
]
