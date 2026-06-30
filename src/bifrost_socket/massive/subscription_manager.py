"""Watchlist query and subscription diff for Massive WS Ingestor (B3: asyncio.to_thread)."""

from __future__ import annotations

import asyncio
import logging
from typing import Optional, Set, Tuple

logger = logging.getLogger(__name__)


def _fetch_watchlist_sync(pg_params: dict) -> Set[str]:
    """Sync psycopg2 query; called via asyncio.to_thread (B3 fix)."""
    import psycopg2

    try:
        conn = psycopg2.connect(**pg_params)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT DISTINCT symbol FROM watchlist"
                    " WHERE sec_type = 'STK' AND optionable = true"
                )
                return {row[0] for row in cur.fetchall()}
        finally:
            conn.close()
    except Exception as e:
        logger.warning("watchlist query failed: %s", e)
        return set()


async def fetch_watchlist_symbols(pg_params: dict) -> Set[str]:
    """Async wrapper — runs psycopg2 query off the event loop (B3 fix)."""
    return await asyncio.to_thread(_fetch_watchlist_sync, pg_params)


def channels_for_symbols(symbols: Set[str], tier: str, trades_enabled: bool) -> str:
    """Build Polygon subscribe params string for given symbols.

    Starter/delayed plans allow options quotes (Q.O) only; minute aggregates (AM.O)
    and trades (T.O) require developer tier — subscribing to them yields WS 1008.
    """
    if not symbols:
        return ""
    tier_norm = (tier or "starter").strip().lower()
    prefixes = ["Q.O:"]
    if tier_norm == "developer":
        prefixes.append("AM.O:")
        if trades_enabled:
            prefixes.append("T.O:")
    parts = []
    for sym in sorted(symbols):
        for p in prefixes:
            parts.append(f"{p}{sym}*")
    return ",".join(parts)


def subscription_diff(
    current: Set[str],
    new: Set[str],
    tier: str,
    trades_enabled: bool,
) -> Tuple[Optional[str], Optional[str]]:
    """Return (channels_to_unsubscribe, channels_to_subscribe); None means no action."""
    removed = current - new
    added = new - current
    unsub = channels_for_symbols(removed, tier, trades_enabled) if removed else None
    sub = channels_for_symbols(added, tier, trades_enabled) if added else None
    return unsub, sub
