"""Redis writer for IB Account Agent health + snapshot (B4 fix: env/config_file in health hash)."""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, Optional

from bifrost_core.ws_client.health import HealthHashWriter

from bifrost_socket.ib.account_agent.redis_keys import (
    IB_ACCOUNT_AGENT_HEALTH_KEY,
    IB_ACCOUNT_NOTIFY_CHANNEL,
    IB_ACCOUNT_SNAPSHOT_KEY,
    IB_ACCOUNT_STREAM_KEY,
    IB_ACCOUNT_STREAM_MAXLEN,
)

logger = logging.getLogger(__name__)


class IbAccountAgentRedisWriter:
    """Writes health hash, account snapshot, and stream entries to Redis.

    B4 fix: health hash always includes 'env' and 'config_file' fields so the
    Monitor dashboard can distinguish Dev from Prod instances.
    """

    def __init__(self, rds: Any, *, env: str = "", config_file: str = "") -> None:
        self._rds = rds
        self._health = HealthHashWriter(
            rds,
            IB_ACCOUNT_AGENT_HEALTH_KEY,
            env=env,
            config_file=config_file,
        )
        self._version = 0

    def write_health(
        self,
        *,
        host_client_id: int,
        host_connected: bool,
        last_msg_ts: float,
        reconnects: int,
        msg_count: int,
        secondary_connected: Optional[bool] = None,
        secondary_client_id: Optional[int] = None,
        host_alive: bool = True,
        host_probe_at: float = 0.0,
        host_probe_ok: bool = False,
        host_probe_interval_sec: float = 0.0,
        secondary_probe_at: float = 0.0,
        secondary_probe_ok: bool = False,
        secondary_probe_interval_sec: float = 0.0,
    ) -> None:
        """Write health hash. env/config_file are set at init via HealthHashWriter."""
        fields: Dict[str, Any] = {
            "connected": host_connected,
            "host_connected": host_connected,
            "host_alive": host_alive,
            "client_id": host_client_id,
            "host_client_id": host_client_id,
            "last_msg_ts": last_msg_ts,
            "reconnects": reconnects,
            "msg_count": msg_count,
            "host_probe_at": host_probe_at,
            "host_probe_ok": host_probe_ok,
            "host_probe_interval_sec": host_probe_interval_sec,
            "secondary_probe_at": secondary_probe_at,
            "secondary_probe_ok": secondary_probe_ok,
            "secondary_probe_interval_sec": secondary_probe_interval_sec,
        }
        if secondary_connected is not None:
            fields["secondary_connected"] = secondary_connected
        if secondary_client_id is not None:
            fields["secondary_client_id"] = secondary_client_id
        try:
            self._health.write(fields)
        except Exception as e:
            err = str(e).lower()
            if "wrong kind" in err or "wrongtype" in err:
                try:
                    self._rds.delete(IB_ACCOUNT_AGENT_HEALTH_KEY)
                    self._health.write(fields)
                except Exception as e2:
                    logger.warning("account agent health write after key delete failed: %s", e2)
            else:
                logger.warning("account agent health write failed: %s", e)

    def write_snapshot(
        self,
        payload: Dict[str, Any],
        *,
        publish_notify: bool = True,
    ) -> None:
        """Write account snapshot to Redis SET, pub/sub notify, and Redis Stream."""
        self._version += 1
        body = dict(payload)
        body["version"] = int(body.get("version") or self._version)
        body["updated_at"] = float(body.get("updated_at") or time.time())
        try:
            raw = json.dumps(body, separators=(",", ":"), default=str)
            self._rds.set(IB_ACCOUNT_SNAPSHOT_KEY, raw)
            if publish_notify:
                self._rds.publish(IB_ACCOUNT_NOTIFY_CHANNEL, str(body["version"]))
        except Exception as e:
            logger.warning("account agent snapshot set failed: %s", e)
        try:
            self._rds.xadd(
                IB_ACCOUNT_STREAM_KEY,
                {
                    "version": str(body["version"]),
                    "updated_at": str(body["updated_at"]),
                    "payload": raw,
                },
                maxlen=IB_ACCOUNT_STREAM_MAXLEN,
                approximate=True,
            )
        except Exception as e:
            logger.warning("account agent stream xadd failed: %s", e)
