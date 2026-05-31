"""Massive (Polygon) Options WebSocket ingestor (B4/B6 fixes, Stream upgrade)."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Dict, Optional, Set

from bifrost_core.ws_client.retry import ReconnectPolicy

from bifrost_socket.config import detect_env, get_pg_conn_params, make_redis_client
from bifrost_socket.massive.pg_sampler import PgSampler
from bifrost_socket.massive.redis_keys import (
    HEALTH_HEARTBEAT_INTERVAL_SEC,
    HEARTBEAT_TIMEOUT_SEC,
    WATCHLIST_POLL_SEC,
)
from bifrost_socket.massive.redis_writer import MassiveRedisWriter
from bifrost_socket.massive.subscription_manager import (
    channels_for_symbols,
    fetch_watchlist_symbols,
    subscription_diff,
)

logger = logging.getLogger(__name__)

_QUEUE_MAX = 10_000


def _get_massive_cfg(config: dict) -> dict:
    """Extract Massive/Polygon settings from YAML config."""
    import os

    m = config.get("massive") or {}
    api_key = (
        os.environ.get("MASSIVE_API_KEY")
        or os.environ.get("POLYGON_API_KEY")
        or m.get("api_key")
        or ""
    ).strip()
    tier = (m.get("tier") or "starter").strip().lower()
    if tier not in ("starter", "developer"):
        tier = "starter"
    feats = m.get("features") or {}
    trades_enabled = bool(feats.get("trades_enabled", tier == "developer"))
    ws_url = (m.get("ws_url") or "wss://socket.polygon.io/options").strip()
    return {
        "api_key": api_key,
        "ws_url": ws_url,
        "tier": tier,
        "trades_enabled": trades_enabled,
    }


def _contract_key_from_ticker(ticker: str) -> Optional[str]:
    """Parse Polygon option ticker (O:NVDA250620C00120000) to contract_key."""
    t = ticker.strip()
    if t.startswith("O:"):
        t = t[2:]
    if len(t) < 16:
        return None
    try:
        sym_end = len(t) - 15
        sym = t[:sym_end]
        date_str = t[sym_end : sym_end + 6]
        right_char = t[sym_end + 6]
        strike_raw = t[sym_end + 7 :]
        yy, mm, dd = date_str[:2], date_str[2:4], date_str[4:6]
        expiry = f"20{yy}{mm}{dd}"
        right = "C" if right_char == "C" else "P"
        strike_str = f"{float(strike_raw) / 1000.0:g}"
        return f"{sym}|OPT|{expiry}|{strike_str}|{right}"
    except Exception:
        return None


class MassiveWsIngestor:
    def __init__(self, cfg: dict) -> None:
        self._cfg = cfg
        self._massive = _get_massive_cfg(cfg)
        env = detect_env(cfg.get("_config_file", ""))
        config_file = str(cfg.get("_config_file") or "")
        rds = make_redis_client(cfg)
        self._writer = MassiveRedisWriter(rds, env=env, config_file=config_file)
        self._pg_params = get_pg_conn_params(cfg)
        self._pg_sampler = PgSampler(self._pg_params)
        self._policy = ReconnectPolicy()
        self._stop = asyncio.Event()
        self._reconnects = 0
        self._msg_count = 0
        self._ws_connected = False
        self._current_symbols: Set[str] = set()

    async def run(self) -> None:
        import signal

        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self._stop.set)

        logger.info(
            "Massive WS ingestor starting (tier=%s, trades=%s)",
            self._massive["tier"],
            self._massive["trades_enabled"],
        )

        health_task = asyncio.create_task(self._health_heartbeat_loop())
        attempt = 0
        try:
            while not self._stop.is_set():
                try:
                    await self._run_session()
                except Exception as e:
                    logger.error("WS session error: %s", e)

                if self._stop.is_set():
                    break

                self._ws_connected = False
                self._reconnects += 1
                attempt += 1
                delay = self._policy.delay_for_attempt(attempt)
                logger.info("Reconnecting in %.1fs (attempt %d)…", delay, attempt)
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    pass
        finally:
            health_task.cancel()
            try:
                await health_task
            except (asyncio.CancelledError, Exception):
                pass

        self._ws_connected = False
        self._writer.write_health(
            connected=False,
            last_msg_ts=time.time(),
            reconnects=self._reconnects,
            msg_count=self._msg_count,
        )
        logger.info(
            "Massive WS ingestor stopped (msgs=%d, reconnects=%d)",
            self._msg_count,
            self._reconnects,
        )

    async def _health_heartbeat_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._writer.write_health(
                    connected=self._ws_connected,
                    last_msg_ts=time.time(),
                    reconnects=self._reconnects,
                    msg_count=self._msg_count,
                )
            except Exception as e:
                logger.debug("health heartbeat: %s", e)
            try:
                await asyncio.wait_for(
                    self._stop.wait(), timeout=HEALTH_HEARTBEAT_INTERVAL_SEC
                )
            except asyncio.TimeoutError:
                pass

    async def _run_session(self) -> None:
        import websockets

        api_key = self._massive["api_key"]
        ws_url = self._massive["ws_url"]  # read-only; never mutate self._massive (B6 fix)

        symbols = await fetch_watchlist_symbols(self._pg_params)
        if not symbols:
            logger.warning("No optionable STK symbols in Watchlist; waiting %ds…", WATCHLIST_POLL_SEC)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=WATCHLIST_POLL_SEC)
            except asyncio.TimeoutError:
                pass
            return

        # Determine effective URL (B6 fix: local variable only, no instance state mutation)
        effective_url = ws_url
        try:
            async with websockets.connect(ws_url, ping_interval=None, ping_timeout=None) as _ws:
                await asyncio.wait_for(_ws.recv(), timeout=10)  # welcome
                await _ws.send(json.dumps({"action": "auth", "params": api_key}))
                _resp = json.loads(await asyncio.wait_for(_ws.recv(), timeout=10))
                if isinstance(_resp, list):
                    _statuses = {m.get("status") for m in _resp if isinstance(m, dict)}
                    if "auth_failed" in _statuses:
                        logger.error("Auth failed — check API key and tier.")
                        return
                    _msgs = " ".join(
                        str(m.get("message", "")).lower()
                        for m in _resp
                        if isinstance(m, dict)
                    )
                    if "delayed" in _msgs:
                        ws_url_delayed = ws_url.replace(
                            "://socket.polygon.io", "://delayed.polygon.io"
                        )
                        if ws_url_delayed == ws_url:
                            ws_url_delayed = "wss://delayed.polygon.io/options"
                        effective_url = ws_url_delayed  # local variable (B6 fix)
                        logger.info("Redirecting to delayed endpoint: %s", effective_url)
        except Exception as e:
            logger.warning("Auth probe failed: %s", e)
            return

        # Real session on effective_url
        channels = channels_for_symbols(symbols, self._massive["tier"], self._massive["trades_enabled"])
        logger.info("Connecting to %s (symbols: %s)", effective_url, ", ".join(sorted(symbols)))

        try:
            async with websockets.connect(
                effective_url, ping_interval=20, ping_timeout=10
            ) as ws:
                # Welcome
                welcome = await asyncio.wait_for(ws.recv(), timeout=10)
                logger.debug("← welcome: %s", str(welcome)[:200])

                # Auth
                await ws.send(json.dumps({"action": "auth", "params": api_key}))
                auth_resp = await asyncio.wait_for(ws.recv(), timeout=10)
                auth_data = json.loads(auth_resp)
                if isinstance(auth_data, list):
                    statuses = {m.get("status") for m in auth_data if isinstance(m, dict)}
                    if "auth_failed" in statuses:
                        logger.error("Auth failed on %s — check API key.", effective_url)
                        return
                logger.info("Auth success")

                # Subscribe
                await ws.send(json.dumps({"action": "subscribe", "params": channels}))
                logger.info("Subscribed: %s", channels[:200])
                self._writer.set_subscriptions(set(channels.split(",")))
                self._ws_connected = True
                self._reconnects = 0
                self._writer.write_health(
                    connected=True,
                    last_msg_ts=time.time(),
                    reconnects=self._reconnects,
                    msg_count=self._msg_count,
                )

                # Consume with watchlist refresh
                queue: asyncio.Queue = asyncio.Queue(maxsize=_QUEUE_MAX)
                recv_task = asyncio.create_task(self._recv_loop(ws, queue))
                process_task = asyncio.create_task(self._process_loop(queue))
                watchlist_task = asyncio.create_task(
                    self._watchlist_refresh_loop(ws, symbols)
                )

                _done, pending = await asyncio.wait(
                    [
                        recv_task,
                        process_task,
                        watchlist_task,
                        asyncio.create_task(self._stop.wait()),
                    ],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for t in pending:
                    t.cancel()
                    try:
                        await t
                    except (asyncio.CancelledError, Exception):
                        pass
        except Exception as e:
            logger.warning("WS session ended: %s", e)
        finally:
            self._ws_connected = False

    async def _recv_loop(self, ws: Any, queue: asyncio.Queue) -> None:
        while not self._stop.is_set():
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=HEARTBEAT_TIMEOUT_SEC)
            except asyncio.TimeoutError:
                logger.warning("No message for %ds, treating as stale", HEARTBEAT_TIMEOUT_SEC)
                return
            except Exception as e:
                logger.debug("recv error: %s", e)
                return
            try:
                if queue.full():
                    try:
                        queue.get_nowait()
                    except asyncio.QueueEmpty:
                        pass
                    logger.debug("Queue full, dropped oldest")
                queue.put_nowait(raw)
            except Exception:
                pass

    async def _process_loop(self, queue: asyncio.Queue) -> None:
        while not self._stop.is_set():
            try:
                raw = await asyncio.wait_for(queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            try:
                msgs = json.loads(raw)
                if not isinstance(msgs, list):
                    msgs = [msgs]
                for msg in msgs:
                    if not isinstance(msg, dict):
                        continue
                    ev = msg.get("ev")
                    if not ev:
                        continue
                    self._msg_count += 1
                    await self._handle_message(msg)
                    if self._msg_count % 500 == 0:
                        self._writer.write_health(
                            connected=True,
                            last_msg_ts=time.time(),
                            reconnects=self._reconnects,
                            msg_count=self._msg_count,
                        )
            except json.JSONDecodeError:
                logger.debug("Bad JSON: %s", raw[:100])
            except Exception as e:
                logger.debug("Process error: %s", e)

    async def _handle_message(self, msg: Dict[str, Any]) -> None:
        ev = msg.get("ev", "")
        sym = msg.get("sym") or msg.get("T") or ""
        ck = _contract_key_from_ticker(sym)
        if not ck:
            return

        data: Dict[str, Any] = {"ev": ev, "sym": sym}
        if ev == "Q":
            data.update({"bid": msg.get("bp"), "ask": msg.get("ap"), "t": msg.get("t")})
        elif ev in ("AM", "A"):
            data.update({
                "c": msg.get("c"), "o": msg.get("o"), "h": msg.get("h"), "l": msg.get("l"),
                "v": msg.get("v"), "t": msg.get("s") or msg.get("t"),
            })
        elif ev == "T":
            data.update({"last": msg.get("p"), "size": msg.get("s"), "t": msg.get("t")})
        else:
            data.update(msg)

        self._writer.write_quote(ck, data)

        if ev == "AM":
            await asyncio.to_thread(self._pg_sampler.maybe_write, ck, data)

    async def _watchlist_refresh_loop(self, ws: Any, initial_symbols: Set[str]) -> None:
        current_symbols = set(initial_symbols)
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=WATCHLIST_POLL_SEC)
                return
            except asyncio.TimeoutError:
                pass

            self._writer.write_health(
                connected=True,
                last_msg_ts=time.time(),
                reconnects=self._reconnects,
                msg_count=self._msg_count,
            )

            new_symbols = await fetch_watchlist_symbols(self._pg_params)
            if not new_symbols or new_symbols == current_symbols:
                continue

            tier = self._massive["tier"]
            trades = self._massive["trades_enabled"]
            unsub, sub = subscription_diff(current_symbols, new_symbols, tier, trades)

            if unsub:
                try:
                    await ws.send(json.dumps({"action": "unsubscribe", "params": unsub}))
                    logger.info("Unsubscribed removed symbols: %s", unsub[:200])
                except Exception as e:
                    logger.warning("Unsubscribe failed: %s", e)

            if sub:
                try:
                    await ws.send(json.dumps({"action": "subscribe", "params": sub}))
                    logger.info("Subscribed new symbols: %s", sub[:200])
                except Exception as e:
                    logger.warning("Subscribe failed: %s", e)

            current_symbols = new_symbols
            self._current_symbols = new_symbols
            new_channels = channels_for_symbols(new_symbols, tier, trades)
            self._writer.set_subscriptions(set(new_channels.split(",")))
