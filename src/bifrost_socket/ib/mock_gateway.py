"""Mock IB gateway — W0 of trade-k8s-native.

Lets bifrost-dev run the IB edge services WITHOUT opening any TWS socket, so a dev
environment consumes ZERO live client_id (IB caps each TWS instance at 32 clients;
dev/stg/prod sharing the same Win11 TWS hosts would otherwise compete for the same
client_id band and risk Error 326).

Each IB edge entrypoint checks ``get_ib_mode(cfg)``; when ``mock`` it runs
``MockIbGateway`` instead of the real connector. The mock:
  - never calls ib_insync ``eConnect`` (no TWS socket, client_id reported as 0)
  - refreshes the same Redis health hash the real service writes, with an extra
    ``mode=mock`` field so the UI can label the slot as a mock connection
  - for the ingestor role, synthesizes a random-walk quote per watchlist symbol so
    the dev Market UI is demonstrably alive

Authority: console/src/lib/architecture/tradeK8sNativeCatalog.ts (wave W0).
"""

from __future__ import annotations

import asyncio
import logging
import random
import signal
import time
from typing import Any, Dict, List, Optional

from bifrost_socket.config import detect_env, get_pg_conn_params, make_redis_client
from bifrost_socket.ib.account_agent.redis_keys import IB_ACCOUNT_AGENT_HEALTH_KEY
from bifrost_socket.ib.account_agent.redis_writer import IbAccountAgentRedisWriter
from bifrost_socket.ib.ingestor.redis_keys import IB_INGESTER_HEALTH_KEY
from bifrost_socket.ib.ingestor.redis_writer import IbIngestorRedisWriter
from bifrost_socket.ib.ingestor.watchlist import fetch_watchlist
from bifrost_socket.ib.operator.redis_keys import IB_OPERATOR_HEALTH_KEY

logger = logging.getLogger(__name__)


def _mock_quote_payload(symbol: str, price: float) -> Dict[str, Any]:
    """Synthetic STK quote payload (mirrors ib_ingestor._quote_payload shape).

    Kept local so the mock gateway never imports the ib_insync chain.
    """
    spread = max(0.01, round(price * 0.0005, 2))
    bid = round(price - spread, 2)
    ask = round(price + spread, 2)
    return {
        "bid": bid,
        "ask": ask,
        "last": round(price, 2),
        "mid": round((bid + ask) / 2.0, 4),
        "ts": time.time(),
        "contract_key": symbol,
        "symbol": symbol,
        "sec_type": "STK",
    }

MOCK_REFRESH_SEC = 5.0
MOCK_CLIENT_ID = 0  # mock never owns a real client_id

# Fallback universe when the dev watchlist / DB is empty.
_DEFAULT_MOCK_SYMBOLS = ["NVDA", "AAPL", "SPY", "QQQ", "TSLA"]
_SEED_PRICE = {"NVDA": 130.0, "AAPL": 220.0, "SPY": 560.0, "QQQ": 480.0, "TSLA": 250.0}


class MockIbGateway:
    """Mock replacement for an IB edge service (ingestor / account_agent / operator)."""

    HEALTH_KEYS = {
        "ib_ingestor": IB_INGESTER_HEALTH_KEY,
        "ib_account_agent": IB_ACCOUNT_AGENT_HEALTH_KEY,
        "ib_operator": IB_OPERATOR_HEALTH_KEY,
    }

    def __init__(self, cfg: Dict[str, Any], role: str) -> None:
        if role not in self.HEALTH_KEYS:
            raise ValueError(f"unknown mock role: {role}")
        self._cfg = cfg
        self._role = role
        self._rds = make_redis_client(cfg)
        self._env = detect_env(cfg.get("_config_file", ""))
        self._config_file = cfg.get("_config_file", "")
        self._stop = asyncio.Event()
        self._prices: Dict[str, float] = {}
        # Ingestor synthesizes quotes; reuse the real writer for schema parity.
        self._ingestor_writer: Optional[IbIngestorRedisWriter] = None
        self._account_writer: Optional[IbAccountAgentRedisWriter] = None
        if role == "ib_ingestor":
            self._ingestor_writer = IbIngestorRedisWriter(
                self._rds, env=self._env, config_file=self._config_file
            )
        elif role == "ib_account_agent":
            self._account_writer = IbAccountAgentRedisWriter(
                self._rds, env=self._env, config_file=self._config_file
            )

    # ── health ────────────────────────────────────────────────────────────────

    def _write_health(self) -> None:
        now = time.time()
        key = self.HEALTH_KEYS[self._role]
        if self._role == "ib_ingestor" and self._ingestor_writer is not None:
            self._ingestor_writer.write_health(
                client_id=MOCK_CLIENT_ID,
                connected=True,
                last_msg_ts=now,
                reconnects=0,
                msg_count=len(self._prices),
                ib_probe_at=now,
                ib_probe_ok=True,
                ib_probe_interval_sec=MOCK_REFRESH_SEC,
                service_heartbeat_interval_sec=MOCK_REFRESH_SEC,
                last_service_heartbeat_at=now,
                next_service_heartbeat_in_s=MOCK_REFRESH_SEC,
            )
        elif self._role == "ib_account_agent" and self._account_writer is not None:
            self._account_writer.write_health(
                host_client_id=MOCK_CLIENT_ID,
                host_connected=True,
                last_msg_ts=now,
                reconnects=0,
                msg_count=0,
                host_probe_at=now,
                host_probe_ok=True,
                host_probe_interval_sec=MOCK_REFRESH_SEC,
                service_heartbeat_interval_sec=MOCK_REFRESH_SEC,
                last_service_heartbeat_at=now,
                next_service_heartbeat_in_s=MOCK_REFRESH_SEC,
            )
        else:
            # Operator: write a minimal canonical health hash directly.
            self._rds.hset(
                key,
                mapping={
                    "connected": "1",
                    "host_connected": "1",
                    "client_id": MOCK_CLIENT_ID,
                    "host_client_id": MOCK_CLIENT_ID,
                    "last_msg_ts": now,
                    "reconnects": 0,
                    "env": self._env,
                    "config_file": self._config_file,
                },
            )
        # Common mock marker for every role (UI labels the slot as a mock).
        self._rds.hset(key, mapping={"mode": "mock", "mock": "1", "client_id": MOCK_CLIENT_ID})

    # ── synthetic quotes (ingestor only) ────────────────────────────────────────

    async def _load_symbols(self) -> List[str]:
        try:
            pg_params = get_pg_conn_params(self._cfg)
            opt_rows, stk_syms = await fetch_watchlist(
                pg_params, max_subscriptions=200, include_stk=True, include_opt=False
            )
            syms = [s for s in stk_syms if s]
            if syms:
                return syms
        except Exception as e:  # noqa: BLE001 — dev mock is best-effort
            logger.debug("mock watchlist fetch failed, using defaults: %s", e)
        return list(_DEFAULT_MOCK_SYMBOLS)

    def _next_price(self, symbol: str) -> float:
        cur = self._prices.get(symbol)
        if cur is None:
            cur = _SEED_PRICE.get(symbol, 100.0)
        drift = cur * random.uniform(-0.002, 0.002)
        cur = max(1.0, round(cur + drift, 2))
        self._prices[symbol] = cur
        return cur

    def _publish_quotes(self, symbols: List[str]) -> None:
        if self._ingestor_writer is None:
            return
        keys = set()
        for sym in symbols:
            price = self._next_price(sym)
            payload = _mock_quote_payload(sym, price)
            self._ingestor_writer.write_quote(sym, payload)
            keys.add(sym)
        self._ingestor_writer.set_subscriptions(keys)

    # ── main loop ───────────────────────────────────────────────────────────────

    async def run(self) -> None:
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._stop.set)
            except NotImplementedError:
                pass

        logger.warning(
            "IB MOCK gateway started role=%s env=%s — NO TWS socket, client_id=%s (W0 trade-k8s-native)",
            self._role,
            self._env or "?",
            MOCK_CLIENT_ID,
        )

        symbols: List[str] = []
        if self._role == "ib_ingestor":
            symbols = await self._load_symbols()
            logger.warning("IB MOCK ingestor synthesizing %d symbols: %s", len(symbols), symbols)

        while not self._stop.is_set():
            try:
                self._write_health()
                if self._role == "ib_ingestor":
                    self._publish_quotes(symbols)
            except Exception as e:  # noqa: BLE001 — keep the mock alive
                logger.warning("mock gateway refresh error: %s", e)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=MOCK_REFRESH_SEC)
            except asyncio.TimeoutError:
                pass

        logger.warning("IB MOCK gateway stopped role=%s", self._role)


def run_mock_gateway(cfg: Dict[str, Any], role: str) -> None:
    """Blocking entrypoint helper for run scripts."""
    asyncio.run(MockIbGateway(cfg, role).run())
