"""IB Operator health hash writer (B4 fix: env/config_file via HealthHashWriter)."""

from __future__ import annotations

import logging
from typing import Any, Dict

from bifrost_core.ib_operator.health_redis import (
    operator_health_dict_to_redis_hash,
    prune_legacy_operator_health_hash_fields,
)
from bifrost_core.ws_client.health import HealthHashWriter

from bifrost_socket.ib.operator.redis_keys import IB_OPERATOR_HEALTH_KEY

logger = logging.getLogger(__name__)


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
        mapping = operator_health_dict_to_redis_hash(health_dict)
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
                    logger.warning("operator health write after key delete failed: %s", e2)
            else:
                logger.warning("operator health write failed: %s", e)

    def write_shutdown(self, health_dict: Dict[str, Any]) -> None:
        """Mark all slots disconnected + service_alive=False on process exit."""
        h = dict(health_dict)
        for slot in ("host", "secondary"):
            sub = h.get(slot)
            if isinstance(sub, dict):
                h[slot] = {**sub, "connected": False}
        h["service_alive"] = False
        mapping = operator_health_dict_to_redis_hash(h)
        try:
            self._rds.hset(IB_OPERATOR_HEALTH_KEY, mapping=mapping)
            prune_legacy_operator_health_hash_fields(self._rds, IB_OPERATOR_HEALTH_KEY)
        except Exception as e:
            logger.warning("operator shutdown health write failed: %s", e)
