"""Watchlist query for IB ingestor.

B3 fix: sync psycopg2 query is wrapped in asyncio.to_thread() so it does not
block the asyncio event loop that drives ib_insync keepalives.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Tuple

logger = logging.getLogger(__name__)


def _fetch_watchlist_sync(
    conn_params: Dict[str, Any],
    max_subscriptions: int,
    include_stk: bool,
    include_opt: bool,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """Synchronous psycopg2 watchlist query; call via asyncio.to_thread().

    Returns (opt_contract_dicts, stk_symbols) capped by max_subscriptions.
    OPT rows are prioritised: they consume the budget first.
    """
    import psycopg2
    from psycopg2.extras import RealDictCursor

    try:
        conn = psycopg2.connect(**conn_params)
        try:
            opt_rows: List[Dict[str, Any]] = []
            stk_syms: List[str] = []
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                if include_opt:
                    # Watchlist OPT entries
                    cur.execute(
                        """
                        SELECT contract_key, symbol, expiry, strike, option_right
                        FROM watchlist
                        WHERE sec_type = 'OPT'
                          AND symbol IS NOT NULL AND TRIM(symbol) <> ''
                          AND expiry IS NOT NULL AND TRIM(expiry) <> ''
                          AND strike IS NOT NULL
                        ORDER BY created_at DESC
                        """
                    )
                    for r in cur.fetchall():
                        ck = (r.get("contract_key") or "").strip()
                        if not ck:
                            continue
                        opt_rows.append({
                            "contract_key": ck,
                            "symbol": (r.get("symbol") or "").strip(),
                            "expiry": (r.get("expiry") or "").strip(),
                            "strike": float(r["strike"]),
                            "option_right": (r.get("option_right") or "C").strip().upper() or "C",
                        })
                    # Append non-zero position OPT contracts (avoid duplicates)
                    cur.execute(
                        """
                        SELECT DISTINCT contract_key, symbol, expiry, strike, option_right
                        FROM account_positions
                        WHERE sec_type = 'OPT'
                          AND position IS NOT NULL AND position != 0
                          AND symbol IS NOT NULL AND TRIM(symbol) <> ''
                          AND expiry IS NOT NULL AND TRIM(expiry) <> ''
                          AND strike IS NOT NULL
                        """
                    )
                    seen_keys = {r["contract_key"] for r in opt_rows}
                    for r in cur.fetchall():
                        ck = (r.get("contract_key") or "").strip()
                        if not ck or ck in seen_keys:
                            continue
                        seen_keys.add(ck)
                        opt_rows.append({
                            "contract_key": ck,
                            "symbol": (r.get("symbol") or "").strip(),
                            "expiry": (r.get("expiry") or "").strip(),
                            "strike": float(r["strike"]),
                            "option_right": (r.get("option_right") or "C").strip().upper() or "C",
                        })

                if include_stk:
                    cur.execute(
                        """
                        SELECT DISTINCT TRIM(symbol) AS sym
                        FROM watchlist
                        WHERE sec_type = 'STK' AND symbol IS NOT NULL AND TRIM(symbol) <> ''
                        ORDER BY sym
                        """
                    )
                    stk_syms = [row["sym"] for row in cur.fetchall() if row.get("sym")]
        finally:
            conn.close()

        budget = max(0, int(max_subscriptions))
        opt_take = min(len(opt_rows), budget)
        opt_sel = opt_rows[:opt_take]
        remaining = budget - opt_take
        stk_sel = stk_syms[: max(0, remaining)]
        return opt_sel, stk_sel

    except Exception as e:
        logger.warning("Watchlist fetch failed: %s", e)
        return [], []


async def fetch_watchlist(
    conn_params: Dict[str, Any],
    max_subscriptions: int,
    include_stk: bool,
    include_opt: bool,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """Async wrapper: runs the sync psycopg2 query in a thread pool (B3 fix)."""
    return await asyncio.to_thread(
        _fetch_watchlist_sync,
        conn_params,
        max_subscriptions,
        include_stk,
        include_opt,
    )
