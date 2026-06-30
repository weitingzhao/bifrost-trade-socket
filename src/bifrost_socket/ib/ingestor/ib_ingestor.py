"""IB market data ingestor.

Subscribes to Watchlist STK/OPT contracts via ib_insync, writes latest quotes to
Redis ``ib:ingester:tick:*``, health hash ``bifrost:health:ws_ib_ingestor`` (including
env/config_file), and pub channel ``ib:ingester:channel``.

Bug fixes vs legacy run_ib_ingestor.py:
  B1 — config path uses resolve_config_path() (env precedence: prod before dev)
  B2 — _heartbeat_loop removed; single outer reconnect path only
  B3 — watchlist query wrapped in asyncio.to_thread() (non-blocking)
  B4 — health hash includes env/config_file via HealthHashWriter
  B5 — MarketIbClient created once per process, not per session
"""

from __future__ import annotations

import asyncio
import logging
import math
import signal
import time
from typing import Any, Dict, List, Optional, Set

from bifrost_core.core.message_center import IbConnectionStatusTracker
from bifrost_core.ws_client.retry import ReconnectPolicy

from bifrost_socket.config import (
    detect_env,
    get_effective_ib_config,
    get_pg_conn_params,
    make_redis_client,
)
from bifrost_socket.ib.connection_lifecycle import (
    REASON_SERVICE_STOPPED,
    REASON_SESSION_ENDED,
    ServiceHeartbeatClock,
    heartbeat_reconnect_slot,
    publish_ingestor_slot,
    resolve_ib_broker_lifecycle,
)
from bifrost_socket.ib.connector.ib_client import MarketIbClient
from bifrost_socket.ib.ingestor.redis_writer import IbIngestorRedisWriter
from bifrost_socket.ib.ingestor.watchlist import fetch_watchlist

logger = logging.getLogger(__name__)

WATCHLIST_POLL_SEC = 60


def _float_or_none(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        v = float(x)
        return None if math.isnan(v) else v
    except (TypeError, ValueError):
        return None


def _quote_payload(contract_key: str, sec_type: str, t: Any) -> Dict[str, Any]:
    bid = _float_or_none(getattr(t, "bid", None))
    ask = _float_or_none(getattr(t, "ask", None))
    last = _float_or_none(getattr(t, "last", None))
    mid: Optional[float] = None
    if bid is not None and ask is not None:
        mid = (bid + ask) / 2.0
    elif last is not None:
        mid = last
    sym = contract_key.split("|", 1)[0] if "|" in contract_key else contract_key
    return {
        "bid": bid,
        "ask": ask,
        "last": last,
        "mid": mid,
        "ts": time.time(),
        "contract_key": contract_key,
        "symbol": sym,
        "sec_type": sec_type,
    }


class IbIngestor:
    """IB market data ingestor process."""

    def __init__(self, cfg: Dict[str, Any]) -> None:
        self._cfg = cfg
        self._rds = make_redis_client(cfg)
        env = detect_env(cfg.get("_config_file", ""))
        self._writer = IbIngestorRedisWriter(
            self._rds,
            env=env,
            config_file=cfg.get("_config_file", ""),
        )
        self._tracker = IbConnectionStatusTracker(self._rds, service="ib_ingestor")
        self._pg_params = get_pg_conn_params(cfg)
        self._stop = asyncio.Event()
        self._session_disconnected = asyncio.Event()
        self._reconnects = 0
        self._msg_count = 0
        self._last_msg_ts = 0.0
        self._lifecycle = resolve_ib_broker_lifecycle(cfg, "ib_ingestor")
        self._hb_clock = ServiceHeartbeatClock(self._lifecycle.service_heartbeat_interval_sec)
        self._probe_interval_sec = self._lifecycle.probe_interval_sec
        self._last_probe_at = 0.0
        self._last_probe_ok = False
        self._client_id = 0
        # Current session's client reference; used by probe loop.
        self._active_client: Optional[MarketIbClient] = None

    # ── settings helpers ──────────────────────────────────────────────────────

    def _st(self) -> Dict[str, Any]:
        raw = self._cfg.get("ib_ingestor") or self._cfg.get("ib_market_ingest")
        return raw if isinstance(raw, dict) else {}

    def _max_subscriptions(self) -> int:
        try:
            return max(1, min(5000, int(self._st().get("max_subscriptions", 200))))
        except (TypeError, ValueError):
            return 200

    def _include_stk(self) -> bool:
        return bool(self._st().get("include_stk", True))

    def _include_opt(self) -> bool:
        return bool(self._st().get("include_opt", True))

    def _push_health(
        self,
        *,
        connected: bool,
        last_msg_ts: float,
        reconnects: int,
        msg_count: int,
        ib_probe_at: float = 0.0,
        ib_probe_ok: bool = False,
        ib_probe_interval_sec: float = 0.0,
        service_heartbeat_reconnect_in_progress: str = "",
        reason: Optional[str] = None,
    ) -> None:
        now = time.time()
        sh = self._hb_clock.redis_fields(
            now,
            reconnect_in_progress=service_heartbeat_reconnect_in_progress,
        )
        self._writer.write_health(
            client_id=self._client_id,
            connected=connected,
            last_msg_ts=last_msg_ts,
            reconnects=reconnects,
            msg_count=msg_count,
            ib_probe_at=ib_probe_at,
            ib_probe_ok=ib_probe_ok,
            ib_probe_interval_sec=ib_probe_interval_sec,
            service_heartbeat_interval_sec=float(sh["service_heartbeat_interval_sec"]),
            last_service_heartbeat_at=float(sh["last_service_heartbeat_at"]),
            next_service_heartbeat_in_s=float(sh["next_service_heartbeat_in_s"]),
            service_heartbeat_reconnect_in_progress=str(
                sh.get("service_heartbeat_reconnect_in_progress") or ""
            ),
        )
        publish_ingestor_slot(
            self._tracker,
            connected=connected,
            client_id=self._client_id or None,
            occurred_at=last_msg_ts,
            reason=reason,
        )

    # ── main entry point ──────────────────────────────────────────────────────

    async def run(self) -> None:
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._stop.set)
            except NotImplementedError:
                pass

        ib_cfg = get_effective_ib_config(self._cfg)
        self._lifecycle = resolve_ib_broker_lifecycle(self._cfg, "ib_ingestor")
        self._hb_clock = ServiceHeartbeatClock(
            self._lifecycle.service_heartbeat_interval_sec,
            last_at=time.time(),
        )
        self._probe_interval_sec = self._lifecycle.probe_interval_sec
        self._client_id = ib_cfg["client_id_market_gateway"]
        host = ib_cfg["host"]
        port = ib_cfg["port_market_data"]
        cid = self._client_id
        policy = ReconnectPolicy.from_config(self._cfg)

        logger.info(
            "IB market gateway starting host=%s port=%s client_id=%s max_subs=%s",
            host,
            port,
            cid,
            self._max_subscriptions(),
        )

        # B5 fix: create client ONCE for the entire process lifetime.
        client = MarketIbClient(host, port, cid, name="IbIngestor")

        attempt = 0
        while not self._stop.is_set():
            try:
                await self._run_session(client)
                attempt = 0
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Ingest session error: %s", e, exc_info=True)
                attempt += 1

            if self._stop.is_set():
                break

            self._reconnects += 1
            delay = policy.delay_for_attempt(attempt)
            self._push_health(
                connected=False,
                last_msg_ts=self._last_msg_ts or time.time(),
                reconnects=self._reconnects,
                msg_count=self._msg_count,
                ib_probe_at=self._last_probe_at,
                ib_probe_ok=False,
                ib_probe_interval_sec=self._probe_interval_sec,
                reason=REASON_SESSION_ENDED,
            )
            logger.info(
                "Next session reconnect in %.1fs (attempt %d)…",
                delay,
                self._reconnects,
            )
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass

        # Final health write on clean shutdown.
        self._push_health(
            connected=False,
            last_msg_ts=self._last_msg_ts or time.time(),
            reconnects=self._reconnects,
            msg_count=self._msg_count,
            reason=REASON_SERVICE_STOPPED,
        )
        logger.info(
            "IB ingestor stopped (messages=%d reconnects=%d)",
            self._msg_count,
            self._reconnects,
        )

    # ── session loop ──────────────────────────────────────────────────────────

    async def _run_session(self, client: MarketIbClient) -> None:
        """Connect and ingest until stop requested or IB disconnects.

        B5 fix: client is passed in and reused — we do NOT create a new MarketIbClient here.
        B2 fix: no _heartbeat_loop; only a probe task that detects disconnect.
        """
        await client.ensure_connected()
        self._session_disconnected.clear()
        self._active_client = client
        self._push_health(
            connected=client.connected_snapshot(),
            last_msg_ts=time.time(),
            reconnects=self._reconnects,
            msg_count=self._msg_count,
            ib_probe_at=self._last_probe_at,
            ib_probe_ok=self._last_probe_ok,
            ib_probe_interval_sec=self._probe_interval_sec,
        )

        probe_task = asyncio.create_task(self._probe_loop(client))
        try:
            while not self._stop.is_set():
                # B3 fix: watchlist query is async (runs in thread pool).
                opt_rows, stk_syms = await fetch_watchlist(
                    self._pg_params,
                    self._max_subscriptions(),
                    self._include_stk(),
                    self._include_opt(),
                )
                # Merge on-demand STK symbols from Redis.
                extra_stk = self._writer.on_demand_stk_symbols()
                if extra_stk:
                    seen = set(stk_syms)
                    for s in extra_stk:
                        if s not in seen:
                            seen.add(s)
                            stk_syms.append(s)

                if not opt_rows and not stk_syms:
                    logger.warning(
                        "No STK/OPT rows in watchlist; retry in %ds", WATCHLIST_POLL_SEC
                    )
                    self._writer.set_subscriptions(set())
                    self._push_health(
                        connected=client.connected_snapshot(),
                        last_msg_ts=self._last_msg_ts or time.time(),
                        reconnects=self._reconnects,
                        msg_count=self._msg_count,
                        ib_probe_at=self._last_probe_at,
                        ib_probe_ok=self._last_probe_ok,
                        ib_probe_interval_sec=self._probe_interval_sec,
                    )
                    should_exit = await self._wait_poll_or_interrupt()
                    if should_exit:
                        return
                    continue

                # Apply subscriptions.
                keys: Set[str] = {r["contract_key"] for r in opt_rows}
                for s in stk_syms:
                    keys.add(f"{s}|STK|||")

                def on_opt(ck: str, ticker: Any) -> None:
                    self._on_tick(ck, "OPT", ticker)

                def on_stk(sym: str, ticker: Any) -> None:
                    self._on_tick(f"{sym}|STK|||", "STK", ticker)

                async def _apply_subs() -> None:
                    c = client.connector
                    if c is None:
                        raise RuntimeError("connector missing after ensure_connected")
                    for sym in list(c.get_subscribed_ticker_symbols()):
                        c.unsubscribe_ticker(sym)
                    for ck in list(c.get_subscribed_option_contract_keys()):
                        c.unsubscribe_option_ticker(ck)
                    if opt_rows:
                        await c.subscribe_option_tickers(opt_rows, on_opt)
                    if stk_syms:
                        await c.subscribe_tickers(stk_syms, on_stk)

                await client._run_on_client_loop(_apply_subs())
                self._writer.set_subscriptions(keys)
                self._push_health(
                    connected=client.connected_snapshot(),
                    last_msg_ts=self._last_msg_ts or time.time(),
                    reconnects=self._reconnects,
                    msg_count=self._msg_count,
                    ib_probe_at=self._last_probe_at,
                    ib_probe_ok=self._last_probe_ok,
                    ib_probe_interval_sec=self._probe_interval_sec,
                )
                logger.info(
                    "Subscribed OPT=%d STK=%d (cap=%d)",
                    len(opt_rows),
                    len(stk_syms),
                    self._max_subscriptions(),
                )

                should_exit = await self._wait_poll_or_interrupt()
                if should_exit:
                    if self._session_disconnected.is_set():
                        logger.warning("IB disconnected — ending session for reconnect")
                    return
        finally:
            self._active_client = None
            probe_task.cancel()
            try:
                await probe_task
            except asyncio.CancelledError:
                pass
            try:
                await client.disconnect()
            except Exception as e:
                logger.debug("disconnect: %s", e)

    def _on_tick(self, contract_key: str, sec_type: str, ticker: Any) -> None:
        payload = _quote_payload(contract_key, sec_type, ticker)
        self._last_msg_ts = float(payload["ts"])
        self._msg_count += 1
        self._writer.write_quote(contract_key, payload)

    async def _probe_loop(self, client: MarketIbClient) -> None:
        """Periodically checks IB connection state.

        B2 fix: this loop only DETECTS disconnect and updates health. It does NOT
        attempt reconnection — that is handled solely by the outer while loop in run().
        """
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(
                    self._stop.wait(),
                    timeout=max(1.0, self._probe_interval_sec),
                )
                break
            except asyncio.TimeoutError:
                pass
            now = time.time()
            reconnect_hint = ""
            if self._hb_clock.tick(now) and not client.connected_snapshot():
                reconnect_hint = ServiceHeartbeatClock.reconnect_hint_part(
                    "Host", self._client_id
                )
                self._push_health(
                    connected=False,
                    last_msg_ts=self._last_msg_ts or now,
                    reconnects=self._reconnects,
                    msg_count=self._msg_count,
                    ib_probe_at=self._last_probe_at,
                    ib_probe_ok=self._last_probe_ok,
                    ib_probe_interval_sec=self._probe_interval_sec,
                    service_heartbeat_reconnect_in_progress=reconnect_hint,
                )
                await heartbeat_reconnect_slot(
                    client,
                    slot_label="Host",
                    client_id=self._client_id,
                    connect_timeout_sec=self._lifecycle.connect_timeout_sec,
                    log_prefix="IB ingestor",
                )
                reconnect_hint = ""
            ok = client.connected_snapshot()
            if not ok:
                self._session_disconnected.set()
            self._last_probe_at = now
            self._last_probe_ok = ok
            try:
                self._push_health(
                    connected=ok,
                    last_msg_ts=self._last_msg_ts or now,
                    reconnects=self._reconnects,
                    msg_count=self._msg_count,
                    ib_probe_at=now,
                    ib_probe_ok=ok,
                    ib_probe_interval_sec=self._probe_interval_sec,
                    service_heartbeat_reconnect_in_progress=reconnect_hint,
                )
            except Exception as e:
                logger.debug("probe health write: %s", e)

    async def _wait_poll_or_interrupt(self) -> bool:
        """Wait up to WATCHLIST_POLL_SEC; return True if session should end."""
        deadline = asyncio.get_event_loop().time() + float(WATCHLIST_POLL_SEC)
        while asyncio.get_event_loop().time() < deadline:
            if self._stop.is_set() or self._session_disconnected.is_set():
                return True
            remaining = deadline - asyncio.get_event_loop().time()
            slice_t = min(1.0, remaining)
            if slice_t <= 0:
                break
            stop_f = asyncio.ensure_future(self._stop.wait())
            disc_f = asyncio.ensure_future(self._session_disconnected.wait())
            done, pending = await asyncio.wait(
                [stop_f, disc_f],
                timeout=slice_t,
                return_when=asyncio.FIRST_COMPLETED,
            )
            for t in pending:
                t.cancel()
            if self._stop.is_set() or self._session_disconnected.is_set():
                return True
        return False
