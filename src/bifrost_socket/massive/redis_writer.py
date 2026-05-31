"""Massive WS Redis writer (B4 fix: env/config_file in health hash; Stream upgrade)."""

from __future__ import annotations

import json
import logging
from typing import Any, Set

from bifrost_core.ws_client.health import HealthHashWriter

from bifrost_socket.massive.redis_keys import (
    MASSIVE_HEALTH_KEY,
    MASSIVE_KEY_PREFIX,
    MASSIVE_KEY_TTL_SEC,
    MASSIVE_META_SUBS,
    MASSIVE_STREAM_KEY,
    MASSIVE_STREAM_MAXLEN,
)

logger = logging.getLogger(__name__)


class MassiveRedisWriter:
    """Writes health hash, quote keys, and stream entries to Redis.

    B4 fix: health hash includes 'env' and 'config_file' via HealthHashWriter.
    Stream upgrade: write_quote uses XADD massive:stream instead of PUBLISH massive:channel.
    """

    def __init__(self, rds: Any, *, env: str = "", config_file: str = "") -> None:
        self._rds = rds
        self._health = HealthHashWriter(
            rds,
            MASSIVE_HEALTH_KEY,
            env=env,
            config_file=config_file,
        )

    def write_quote(self, contract_key: str, data: dict) -> None:
        """Write quote to Redis SET + XADD stream."""
        key = MASSIVE_KEY_PREFIX + contract_key
        raw = json.dumps(data, default=str)
        try:
            self._rds.set(key, raw, ex=MASSIVE_KEY_TTL_SEC)
        except Exception as e:
            logger.warning("write_quote set failed for %s: %s", contract_key, e)
        try:
            self._rds.xadd(
                MASSIVE_STREAM_KEY,
                {
                    "contract_key": contract_key,
                    "ev": str(data.get("ev") or ""),
                    "payload": raw,
                },
                maxlen=MASSIVE_STREAM_MAXLEN,
                approximate=True,
            )
        except Exception as e:
            logger.warning("write_quote xadd failed for %s: %s", contract_key, e)

    def write_health(
        self,
        *,
        connected: bool,
        last_msg_ts: float,
        reconnects: int,
        msg_count: int,
    ) -> None:
        fields = {
            "connected": connected,
            "last_msg_ts": last_msg_ts,
            "reconnects": reconnects,
            "msg_count": msg_count,
        }
        try:
            self._health.write(fields)
        except Exception as e:
            err = str(e).lower()
            if "wrong kind" in err or "wrongtype" in err:
                try:
                    self._rds.delete(MASSIVE_HEALTH_KEY)
                    self._health.write(fields)
                except Exception as e2:
                    logger.warning("massive health write after key delete failed: %s", e2)
            else:
                logger.warning("massive health write failed: %s", e)

    def set_subscriptions(self, channels: Set[str]) -> None:
        try:
            pipe = self._rds.pipeline()
            pipe.delete(MASSIVE_META_SUBS)
            if channels:
                pipe.sadd(MASSIVE_META_SUBS, *channels)
            pipe.execute()
        except Exception as e:
            logger.warning("set_subscriptions failed: %s", e)
