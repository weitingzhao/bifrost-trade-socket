"""IB Operator Redis protocol v1: ops, envelopes, parsing."""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

PROTOCOL_VERSION = "1"

ALL_OPS: Tuple[str, ...] = (
    "fetch_bars",
    "fetch_bars_range",
    "fetch_option_expirations",
    "fetch_option_snapshot",
    "fetch_executions",
    "fetch_accounts_snapshot",
    "place_stock_order",
    "ping",
    "disconnect_all",
    "reconnect_all",
)


@dataclass
class CommandMessage:
    req_id: str
    version: str
    op: str
    payload: Dict[str, Any]
    caller: str
    deadline_ms: Optional[int]
    stream_id: str

    def is_expired(self) -> bool:
        if self.deadline_ms is None:
            return False
        return int(time.time() * 1000) > int(self.deadline_ms)


def new_req_id() -> str:
    return str(uuid.uuid4())


def result_key(prefix: str, req_id: str) -> str:
    return f"{prefix}{req_id}"


def dumps_result(envelope: Dict[str, Any], *, max_bytes: int) -> Tuple[Optional[str], Optional[str]]:
    """Return (json_str, error). If too large, error is set."""
    try:
        s = json.dumps(envelope, separators=(",", ":"), default=str)
    except (TypeError, ValueError) as e:
        return None, f"json_encode_error:{e}"
    if len(s.encode("utf-8")) > max_bytes:
        return None, "result_too_large"
    return s, None


def loads_json(s: str) -> Dict[str, Any]:
    return json.loads(s) if s else {}


def parse_stream_fields(
    fields: Dict[str, str],
    *,
    stream_id: str,
) -> Tuple[Optional[CommandMessage], Optional[str]]:
    """Parse Redis Stream entry fields. Return (msg, error_reason)."""
    req_id = (fields.get("req_id") or "").strip()
    if not req_id:
        return None, "missing_req_id"
    ver = (fields.get("v") or PROTOCOL_VERSION).strip() or PROTOCOL_VERSION
    if ver != PROTOCOL_VERSION:
        return None, f"unsupported_version:{ver}"
    op = (fields.get("op") or "").strip()
    if not op:
        return None, "missing_op"
    if op not in ALL_OPS:
        return None, f"unknown_op:{op}"
    payload_raw = fields.get("payload") or "{}"
    try:
        payload = json.loads(payload_raw) if isinstance(payload_raw, str) else {}
        if not isinstance(payload, dict):
            payload = {}
    except json.JSONDecodeError:
        return None, "invalid_payload_json"
    caller = (fields.get("caller") or "").strip() or "unknown"
    deadline_ms: Optional[int] = None
    dm = fields.get("deadline_ms")
    if dm is not None and str(dm).strip() != "":
        try:
            deadline_ms = int(dm)
        except (TypeError, ValueError):
            return None, "invalid_deadline_ms"
    return (
        CommandMessage(
            req_id=req_id,
            version=ver,
            op=op,
            payload=payload,
            caller=caller,
            deadline_ms=deadline_ms,
            stream_id=stream_id,
        ),
        None,
    )
