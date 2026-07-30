"""1-minute PostgreSQL sampler for Massive WS option snapshots.

P9: ``public.option_snapshots`` was dropped. REST/plugin ingest owns
``market.option_snapshot``. This sampler is a no-op (Redis path remains).
"""

from __future__ import annotations

import logging
from typing import Any, Dict

from bifrost_socket.massive.redis_keys import PG_SAMPLE_INTERVAL_SEC

logger = logging.getLogger(__name__)

# Re-export for callers that may read the interval constant via this module.
__all__ = ["PgSampler", "PG_SAMPLE_INTERVAL_SEC"]


class PgSampler:
    """Retired PG writer — Massive WS samples stay in Redis only (P9)."""

    def __init__(self, pg_params: dict) -> None:
        self._pg_params = pg_params
        self._last_write: Dict[str, float] = {}

    def maybe_write(self, contract_key: str, data: Dict[str, Any]) -> bool:
        """No-op since P9 dropped ``public.option_snapshots``."""
        return False
