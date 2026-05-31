"""Merge ib_operator: YAML block with defaults."""

from __future__ import annotations

import logging
from typing import Any, Dict

from bifrost_socket.ib.operator.redis_keys import (
    IB_OPERATOR_CMD_STREAM,
    IB_OPERATOR_CONSUMER_GROUP,
    IB_OPERATOR_HEALTH_KEY,
    IB_OPERATOR_RESULT_PREFIX,
    IB_OPERATOR_RESULT_TTL_SEC,
)

logger = logging.getLogger(__name__)

# Legacy health key names — still appear in old Redis hashes.
_LEGACY_HEALTH_KEYS = (
    "bifrost:health:ib_operator",
    "ib:operator:meta:health",
)


def effective_ib_operator_settings(config: Dict[str, Any]) -> Dict[str, Any]:
    """Return resolved IB Operator settings (stream, group, TTLs, keys).

    ``enabled`` defaults to True when a Redis URL is available and
    ``ib_operator.enabled`` is not explicitly False.

    Reads ``ib_gateway`` as a fallback if ``ib_operator`` is absent (deprecated).
    """
    raw = config.get("ib_operator")
    if raw is None and config.get("ib_gateway") is not None:
        logger.warning("YAML key ib_gateway is deprecated; rename to ib_operator.")
        raw = config.get("ib_gateway")
    raw = raw if isinstance(raw, dict) else {}

    from bifrost_socket.config import make_redis_client  # noqa: F401 — just need URL

    # Derive enabled from whether redis block is configured.
    redis_cfg = config.get("redis") or {}
    redis_enabled = bool(redis_cfg.get("enabled") or redis_cfg.get("url") or redis_cfg.get("host"))

    explicit_enabled = raw.get("enabled")
    if explicit_enabled is False:
        enabled = False
    elif explicit_enabled is True:
        enabled = True
    else:
        enabled = redis_enabled

    raw_hk = (raw.get("health_key") or "").strip()
    if raw_hk and raw_hk not in (IB_OPERATOR_HEALTH_KEY,) + _LEGACY_HEALTH_KEYS:
        logger.warning(
            "ib_operator.health_key=%r ignored; health is written only to %s",
            raw_hk,
            IB_OPERATOR_HEALTH_KEY,
        )
    elif raw_hk in _LEGACY_HEALTH_KEYS:
        logger.warning(
            "ib_operator.health_key=%s is deprecated; health is written only to %s",
            raw_hk,
            IB_OPERATOR_HEALTH_KEY,
        )

    return {
        "enabled": bool(enabled),
        "stream": (raw.get("stream") or IB_OPERATOR_CMD_STREAM).strip() or IB_OPERATOR_CMD_STREAM,
        "consumer_group": (raw.get("consumer_group") or IB_OPERATOR_CONSUMER_GROUP).strip() or IB_OPERATOR_CONSUMER_GROUP,
        "result_prefix": (raw.get("result_prefix") or IB_OPERATOR_RESULT_PREFIX).strip() or IB_OPERATOR_RESULT_PREFIX,
        "health_key": IB_OPERATOR_HEALTH_KEY,
        "result_ttl_sec": int(raw.get("result_ttl_sec") or IB_OPERATOR_RESULT_TTL_SEC),
        "request_timeout_sec": float(raw.get("request_timeout_sec") or 120),
        "bars_backfill_request_timeout_sec": float(raw.get("bars_backfill_request_timeout_sec") or 7200),
        "health_refresh_sec": float(raw.get("health_refresh_sec") or 30),
        "max_result_bytes": int(raw.get("max_result_bytes") or (4 * 1024 * 1024)),
        "block_ms": int(raw.get("block_ms") or 5000),
        "use_for_celery_bars": bool(raw.get("use_for_celery_bars")),
    }
