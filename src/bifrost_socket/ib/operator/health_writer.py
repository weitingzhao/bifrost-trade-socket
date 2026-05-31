"""IB Operator health hash writer (B4 fix: env/config_file via HealthHashWriter)."""

from __future__ import annotations

import time
from typing import Any, Dict, Optional

from bifrost_core.ws_client.health import HealthHashWriter

from bifrost_socket.ib.operator.redis_keys import IB_OPERATOR_HEALTH_KEY

# Written by older versions; removed on each health refresh so HGETALL is unambiguous.
_LEGACY_FIELDS: tuple = (
    "operator_connected",
    "operator_client_id",
    "operator_last_error",
    "operator_alive",
    "account2_present",
    "account2_connected",
    "account2_client_id",
    "account2_last_error",
    "account2_reconnects",
    "reconnects",
)


def prune_legacy_operator_health_hash_fields(r: Any, key: str) -> None:
    """HDEL deprecated hash fields after writing canonical host_/secondary_ keys."""
    try:
        r.hdel(key, *_LEGACY_FIELDS)
    except Exception:
        pass


def _jsonish_connected(v: Any) -> bool:
    if v is True:
        return True
    if v is False or v is None:
        return False
    if isinstance(v, (int, float)):
        return v != 0
    if isinstance(v, str):
        return v.strip().lower() in ("1", "true", "yes")
    return False


def _flatten_health(h: Dict[str, Any]) -> Dict[str, str]:
    """Flatten executor.health_dict() to Redis hash string fields."""
    host = h.get("host") if isinstance(h.get("host"), dict) else {}
    if not host and isinstance(h.get("operator"), dict):
        host = h["operator"]
    last_cmd = float(h.get("last_cmd_ts", 0) or 0)
    cmd_count = int(h.get("cmd_count", 0) or 0)
    svc_alive = _jsonish_connected(h.get("service_alive", True))
    sh_iv = float(h.get("service_heartbeat_interval_sec") or 0)
    sh_last = float(h.get("last_service_heartbeat_at") or 0)
    sh_next = float(h.get("next_service_heartbeat_in_s") or 0)
    now = time.time()
    mapping: Dict[str, str] = {
        "host_connected": "1" if _jsonish_connected(host.get("connected")) else "0",
        "host_client_id": str(int(host.get("client_id") or 0)),
        "host_last_error": "" if host.get("last_error") is None else str(host.get("last_error")),
        "host_alive": "1" if svc_alive else "0",
        "host_reconnects": str(int(host.get("reconnects") or 0)),
        "host_ib_probe_at": str(float(host.get("ib_probe_at") or 0)),
        "host_ib_probe_ok": "1" if _jsonish_connected(host.get("ib_probe_ok")) else "0",
        "host_ib_probe_interval_sec": str(float(host.get("ib_probe_interval_sec") or 0)),
        "msg_count": str(cmd_count),
        "last_msg_ts": str(last_cmd if last_cmd > 0 else now),
        "service_heartbeat_interval_sec": str(sh_iv),
        "last_service_heartbeat_at": str(sh_last),
        "next_service_heartbeat_in_s": str(sh_next),
        "service_heartbeat_reconnect_in_progress": (
            "" if h.get("service_heartbeat_reconnect_in_progress") is None
            else str(h.get("service_heartbeat_reconnect_in_progress"))
        ),
    }
    sec = h.get("secondary")
    if sec is not None and isinstance(sec, dict):
        mapping["secondary_present"] = "1"
        mapping["secondary_connected"] = "1" if _jsonish_connected(sec.get("connected")) else "0"
        mapping["secondary_client_id"] = str(int(sec.get("client_id") or 0))
        mapping["secondary_last_error"] = (
            "" if sec.get("last_error") is None else str(sec.get("last_error"))
        )
        mapping["secondary_reconnects"] = str(int(sec.get("reconnects") or 0))
        mapping["secondary_ib_probe_at"] = str(float(sec.get("ib_probe_at") or 0))
        mapping["secondary_ib_probe_ok"] = "1" if _jsonish_connected(sec.get("ib_probe_ok")) else "0"
        mapping["secondary_ib_probe_interval_sec"] = str(float(sec.get("ib_probe_interval_sec") or 0))
    else:
        mapping["secondary_present"] = "0"
        mapping["secondary_connected"] = "0"
        mapping["secondary_client_id"] = "0"
        mapping["secondary_last_error"] = ""
        mapping["secondary_reconnects"] = "0"
        mapping["secondary_ib_probe_at"] = "0"
        mapping["secondary_ib_probe_ok"] = "0"
        mapping["secondary_ib_probe_interval_sec"] = "0"
    return mapping


class IbOperatorHealthWriter:
    """Writes operator health hash; B4 fix: env/config_file via HealthHashWriter."""

    def __init__(self, rds: Any, *, env: str = "", config_file: str = "") -> None:
        self._rds = rds
        self._health = HealthHashWriter(
            rds,
            IB_OPERATOR_HEALTH_KEY,
            env=env,
            config_file=config_file,
        )

    def write(self, health_dict: Dict[str, Any]) -> None:
        mapping = _flatten_health(health_dict)
        try:
            # Do not DELETE hash; HSET merges — ops control fields on this key survive heartbeat refresh.
            self._health.write(mapping)
            prune_legacy_operator_health_hash_fields(self._rds, IB_OPERATOR_HEALTH_KEY)
        except Exception as e:
            err = str(e).lower()
            if "wrong kind" in err or "wrongtype" in err:
                try:
                    self._rds.delete(IB_OPERATOR_HEALTH_KEY)
                    self._health.write(mapping)
                except Exception as e2:
                    import logging
                    logging.getLogger(__name__).warning(
                        "operator health write after key delete failed: %s", e2
                    )
            else:
                import logging
                logging.getLogger(__name__).warning("operator health write failed: %s", e)

    def write_shutdown(self, health_dict: Dict[str, Any]) -> None:
        """Mark all slots disconnected + service_alive=False on process exit."""
        h = dict(health_dict)
        for slot in ("host", "secondary"):
            sub = h.get(slot)
            if isinstance(sub, dict):
                h[slot] = {**sub, "connected": False}
        h["service_alive"] = False
        mapping = _flatten_health(h)
        try:
            self._rds.hset(IB_OPERATOR_HEALTH_KEY, mapping=mapping)
            prune_legacy_operator_health_hash_fields(self._rds, IB_OPERATOR_HEALTH_KEY)
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning("operator shutdown health write failed: %s", e)
