"""Redis writer for IB ingestor ticks and health (B4 fix: env/config_file in health hash)."""

from __future__ import annotations

import json
from typing import Any, Dict, Set

from bifrost_core.ws_client.health import HealthHashWriter

from bifrost_socket.ib.ingestor.redis_keys import (
    IB_INGESTER_CHANNEL,
    IB_INGESTER_HEALTH_KEY,
    IB_INGESTER_SUBSCRIPTIONS_KEY,
    IB_INGESTER_TICK_PREFIX,
    IB_INGESTER_TICK_TTL_SEC,
)


class IbIngestorRedisWriter:
    """Writes tick data, health hash, and subscriptions set to Redis.

    B4 fix: health hash always includes 'env' and 'config_file' fields so the
    Monitor dashboard can distinguish Dev from Prod instances.
    """

    def __init__(self, rds: Any, *, env: str = "", config_file: str = "") -> None:
        self._rds = rds
        self._health = HealthHashWriter(
            rds,
            IB_INGESTER_HEALTH_KEY,
            env=env,
            config_file=config_file,
        )

    def write_quote(self, contract_key: str, data: Dict[str, Any]) -> None:
        """Write tick hash (with TTL) and publish to IB ingestor channel."""
        key = IB_INGESTER_TICK_PREFIX + contract_key
        self._rds.set(key, json.dumps(data, default=str), ex=IB_INGESTER_TICK_TTL_SEC)
        self._rds.publish(
            IB_INGESTER_CHANNEL,
            json.dumps(
                {"contract_key": contract_key, "ts": data.get("ts")},
                default=str,
            ),
        )

    def write_health(
        self,
        *,
        client_id: int,
        connected: bool,
        last_msg_ts: float,
        reconnects: int,
        msg_count: int,
        ib_probe_at: float = 0.0,
        ib_probe_ok: bool = False,
        ib_probe_interval_sec: float = 0.0,
    ) -> None:
        """Write health hash. env/config_file are set once at init via HealthHashWriter."""
        self._health.write({
            "client_id": client_id,
            "connected": connected,
            "last_msg_ts": last_msg_ts,
            "reconnects": reconnects,
            "msg_count": msg_count,
            "ib_probe_at": ib_probe_at,
            "ib_probe_ok": ib_probe_ok,
            "ib_probe_interval_sec": ib_probe_interval_sec,
        })

    def set_subscriptions(self, contract_keys: Set[str]) -> None:
        """Replace the subscriptions set atomically."""
        pipe = self._rds.pipeline()
        pipe.delete(IB_INGESTER_SUBSCRIPTIONS_KEY)
        if contract_keys:
            pipe.sadd(IB_INGESTER_SUBSCRIPTIONS_KEY, *sorted(contract_keys))
        pipe.execute()

    def on_demand_stk_symbols(self) -> list:
        """Read extra STK symbols from Redis SET for on-demand subscription."""
        try:
            raw = self._rds.smembers(IB_INGESTER_ON_DEMAND_STK)
        except Exception:
            return []
        return [str(x).strip().upper() for x in (raw or []) if str(x).strip()]
