"""1-minute PostgreSQL sampler for Massive WS option snapshots."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict

from bifrost_socket.massive.redis_keys import PG_SAMPLE_INTERVAL_SEC

logger = logging.getLogger(__name__)


class PgSampler:
    """Writes at most one snapshot row per contract_key per minute to PostgreSQL."""

    def __init__(self, pg_params: dict) -> None:
        self._pg_params = pg_params
        self._last_write: Dict[str, float] = {}

    def maybe_write(self, contract_key: str, data: Dict[str, Any]) -> bool:
        """Synchronous write; call via asyncio.to_thread from async context."""
        now = time.time()
        if now - self._last_write.get(contract_key, 0) < PG_SAMPLE_INTERVAL_SEC:
            return False
        try:
            import psycopg2

            conn = psycopg2.connect(**self._pg_params)
            try:
                with conn.cursor() as cur:
                    t_raw = data.get("t", now)
                    snapshot_ts = datetime.fromtimestamp(
                        t_raw / 1000 if t_raw > 1e12 else t_raw,
                        tz=timezone.utc,
                    )
                    cur.execute(
                        """
                        INSERT INTO option_snapshots (
                            contract_key, snapshot_ts,
                            iv, delta, gamma, theta, vega, open_interest,
                            source, created_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'massive_ws', now())
                        ON CONFLICT (contract_key, snapshot_ts) DO UPDATE SET
                          iv = COALESCE(EXCLUDED.iv, option_snapshots.iv),
                          delta = COALESCE(EXCLUDED.delta, option_snapshots.delta),
                          gamma = COALESCE(EXCLUDED.gamma, option_snapshots.gamma),
                          theta = COALESCE(EXCLUDED.theta, option_snapshots.theta),
                          vega = COALESCE(EXCLUDED.vega, option_snapshots.vega),
                          open_interest = COALESCE(EXCLUDED.open_interest, option_snapshots.open_interest)
                        """,
                        (
                            contract_key,
                            snapshot_ts,
                            data.get("iv"),
                            data.get("delta"),
                            data.get("gamma"),
                            data.get("theta"),
                            data.get("vega"),
                            data.get("oi"),
                        ),
                    )
                conn.commit()
                self._last_write[contract_key] = now
                return True
            finally:
                conn.close()
        except Exception as e:
            logger.debug("PG sample write failed for %s: %s", contract_key, e)
            return False
