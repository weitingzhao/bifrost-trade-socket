"""Monitor-side IB clients for account- and market-related operations.

AccountIbClient / MarketIbClient are thin managers around IBConnector that:
- keep a long-lived connection per client_id (per process)
- expose high-level async methods for the status server
- track basic health info for UI (connected, last_error, timestamps)
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from bifrost_socket.ib.connector.ib_connector_legacy import IBConnector, IBConnectionDroppedError

logger = logging.getLogger(__name__)


class BaseMonitorIbClient:
    """Base class for monitor-side IB clients.

    One instance per (host, port, client_id) in the status server process.
    """

    def __init__(self, host: str, port: int, client_id: int, *, name: str) -> None:
        self.host = (host or "127.0.0.1").strip() or "127.0.0.1"
        self.port = int(port)
        self.client_id = int(client_id)
        self.name = name
        self._connector: Optional[IBConnector] = None
        self._lock: Optional[asyncio.Lock] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._loop_ready = threading.Event()
        self._loop_guard = threading.Lock()
        self._loop_thread: Optional[threading.Thread] = None
        self._connected_state = False
        self.last_error: Optional[str] = None
        self.last_connected_at: Optional[float] = None
        self.last_disconnected_at: Optional[float] = None
        self.reconnects = 0
        self._ensure_loop()

    @property
    def connector(self) -> Optional[IBConnector]:
        return self._connector

    @property
    def connected(self) -> bool:
        """True when IBConnector reports live API (``ib.isConnected()`` + internal flag).

        Do not rely on ``_connected_state`` alone: health Redis must match ib_insync, or
        ``host_connected`` can stay 0 while logs show ``Connected`` / ``API connection ready``.

        Call this from coroutines that run **on this client's event loop** (e.g. inside
        ``_ensure_connected_impl``). For Redis health / other threads use ``connected_snapshot``.
        """
        conn = self._connector
        if conn is not None:
            try:
                return bool(conn.is_connected)
            except Exception:
                return self._connected_state
        return self._connected_state

    _POST_CONNECT_GRACE_SEC = 10.0

    def connected_snapshot(self) -> bool:
        """Read live API state on this client's event loop (safe from any thread).

        ``run_ib_operator`` writes Redis health from its main thread; ``ib.isConnected()`` is not
        reliable across threads, so ``host_connected`` / ``secondary_connected`` could stay 0
        after a successful connect until this path runs on the IB loop.
        """
        self._ensure_loop()
        loop = self._loop
        if loop is None:
            return self._connected_state

        async def _read() -> bool:
            conn = self._connector
            if conn is None:
                return False
            try:
                api = bool(conn.is_connected)
            except Exception:
                return self._connected_state
            if api:
                return True
            # api is False — only trust _connected_state during post-connect grace window
            # (ib_insync may lag setting _apiReady right after connectAsync returns).
            if self._connected_state and self.last_connected_at is not None:
                elapsed = time.time() - self.last_connected_at
                if elapsed < self._POST_CONNECT_GRACE_SEC:
                    return True
            self._connected_state = False
            return False

        try:
            fut = asyncio.run_coroutine_threadsafe(_read(), loop)
            return bool(fut.result(timeout=5.0))
        except Exception:
            return self._connected_state

    def _on_ib_disconnected_event(self) -> None:
        """ib_insync disconnectedEvent handler — immediately clear _connected_state."""
        was = self._connected_state
        self._connected_state = False
        self.last_disconnected_at = time.time()
        if was:
            logger.warning(
                "[monitor_ib] %s disconnectedEvent → _connected_state cleared", self.name
            )

    def _ensure_loop(self) -> None:
        if self._loop is not None and self._loop.is_running():
            return
        with self._loop_guard:
            if self._loop is not None and self._loop.is_running():
                return
            self._loop_ready.clear()

            def _run_loop() -> None:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                self._loop = loop
                self._lock = asyncio.Lock()
                self._loop_ready.set()
                try:
                    loop.run_forever()
                finally:
                    loop.close()

            self._loop_thread = threading.Thread(
                target=_run_loop,
                daemon=True,
                name=f"monitor-ib-{self.name}",
            )
            self._loop_thread.start()
        if not self._loop_ready.wait(timeout=2.0):
            raise RuntimeError(f"{self.name} IB loop failed to start within 2s")

    async def _run_on_client_loop(self, coro: Any) -> Any:
        self._ensure_loop()
        loop = self._loop
        if loop is None:
            raise RuntimeError(f"{self.name} loop not available")
        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:  # pragma: no cover - defensive
            current_loop = None
        if current_loop is loop:
            return await coro
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        return await asyncio.wrap_future(future)

    async def ensure_connected(self, *, max_connect_attempts: Optional[int] = None) -> None:
        """Ensure there is an active IB connection.

        Lazily creates IBConnector and connects. Subsequent calls are no-ops when already connected.

        ``max_connect_attempts``:
        - ``None`` (default): up to 10 client_id steps + multi-port (startup / full reconnect).
        - ``1``: single attempt per caller (service heartbeat tick — no extra retries until next tick).
        """
        await self._run_on_client_loop(self._ensure_connected_impl(max_connect_attempts))

    async def _ensure_connected_impl(self, max_connect_attempts: Optional[int] = None) -> None:
        if self.connected:
            return
        assert self._lock is not None
        async with self._lock:
            if self.connected:
                return
            heartbeat = max_connect_attempts == 1
            limit = 10 if max_connect_attempts is None else max(1, int(max_connect_attempts))
            port_steps = 1 if heartbeat else 5
            if heartbeat:
                logger.info(
                    "[monitor_ib] heartbeat reconnect %s → %s:%s clientId=%s (single attempt)",
                    self.name,
                    self.host,
                    self.port,
                    self.client_id,
                )
            else:
                logger.info(
                    "[monitor_ib] connecting %s to %s:%s clientId=%s (will try +1, +2, ... if in use)",
                    self.name,
                    self.host,
                    self.port,
                    self.client_id,
                )
            self._connector = IBConnector(host=self.host, port=self.port, client_id=self.client_id)
            ok = await self._connector.connect(max_attempts=limit, max_port_steps=port_steps)
            if not ok:
                self.last_error = "connect_failed"
                self._connected_state = False
                logger.error(
                    "[monitor_ib] %s connect_failed host=%s port=%s clientId=%s",
                    self.name,
                    self.host,
                    self.port,
                    self.client_id,
                )
                # Drop connector on failure so next call can retry cleanly.
                self._connector = None
                raise RuntimeError(f"{self.name} failed to connect to IB (see logs for details)")
            self.client_id = int(getattr(self._connector, "client_id", self.client_id))
            self.last_error = None
            if self.last_disconnected_at is not None:
                self.reconnects += 1
            self.last_connected_at = time.time()
            self._connected_state = True
            self._connector.ib.disconnectedEvent += self._on_ib_disconnected_event
            logger.info(
                "[monitor_ib] %s connected host=%s port=%s clientId=%s",
                self.name,
                self.host,
                self.port,
                self.client_id,
            )

    async def disconnect(self) -> None:
        """Disconnect from IB (if connected)."""
        await self._run_on_client_loop(self._disconnect_impl())

    async def _disconnect_impl(self) -> None:
        assert self._lock is not None
        async with self._lock:
            if not self._connector:
                self._connected_state = False
                return
            try:
                logger.info(
                    "[monitor_ib] disconnecting %s clientId=%s", self.name, self.client_id
                )
                await self._connector.disconnect()
                self.last_disconnected_at = time.time()
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning(
                    "[monitor_ib] %s disconnect error: %s", self.name, exc, exc_info=True
                )
            finally:
                self._connector = None
                self._connected_state = False

    async def set_commission_report_callback(self, callback: Any) -> None:
        await self._run_on_client_loop(self._set_commission_report_callback_impl(callback))

    async def _set_commission_report_callback_impl(self, callback: Any) -> None:
        if callback is not None:
            await self._ensure_connected_impl()
        if self._connector is not None:
            self._connector.set_commission_report_callback(callback)


class AccountIbClient(BaseMonitorIbClient):
    """Monitor-side account client: executions/trades and (optionally) account snapshots."""

    async def fetch_executions(self, days: int) -> List[Dict[str, Any]]:
        return await self._run_on_client_loop(self._fetch_executions_impl(days))

    async def _fetch_executions_impl(self, days: int) -> List[Dict[str, Any]]:
        """Fetch executions for all managed accounts over the last `days` days."""
        await self._ensure_connected_impl()
        assert self.connector is not None  # for type checkers
        try:
            account_ids = self.connector.get_managed_accounts()
            if not account_ids:
                account_ids = [""]
            all_execs: List[Dict[str, Any]] = []
            for acc in account_ids:
                exec_list = await self.connector.get_executions_async(
                    account=acc or None,
                    since_days=days,
                )
                if exec_list:
                    all_execs.extend(exec_list)
            logger.info(
                "[monitor_ib] AccountIbClient.fetch_executions: %s executions over %s days",
                len(all_execs),
                days,
            )
            self.last_error = None
            return all_execs
        except Exception as e:
            self.last_error = str(e)
            logger.warning(
                "[monitor_ib] AccountIbClient.fetch_executions failed: %s", e, exc_info=True
            )
            return []

    async def fetch_accounts_snapshot(self) -> List[Dict[str, Any]]:
        return await self._run_on_client_loop(self._fetch_accounts_snapshot_impl())

    async def _fetch_accounts_snapshot_impl(self) -> List[Dict[str, Any]]:
        """R-A1: 从 IB 拉取多账户摘要与持仓，返回与 persistence（accounts_sync / PostgreSQLSink）一致的 accounts_snapshot 列表形状。
        供监控端「刷新」按钮通过长连接 Account Client 立即拉取并写库。
        """
        await self._ensure_connected_impl()
        assert self.connector is not None
        try:
            account_ids = self.connector.get_managed_accounts()
            if not account_ids:
                logger.warning(
                    "[monitor_ib] AccountIbClient.fetch_accounts_snapshot: get_managed_accounts returned 0"
                )
                return []
            all_positions = await self.connector.get_positions(account=None)
            accounts_list: List[Dict[str, Any]] = []
            for account_id in account_ids:
                values = await self.connector.get_account_summary(account=account_id)
                summary: Dict[str, Any] = {}
                for v in values:
                    if getattr(v, "tag", None) and getattr(v, "value", None) is not None:
                        summary[v.tag] = v.value
                if account_id:
                    summary["account"] = account_id
                acct_positions = [
                    p for p in all_positions if getattr(p, "account", None) == account_id
                ]
                pos_dicts = [
                    self.connector.position_to_dict(p) for p in acct_positions
                ]
                accounts_list.append({
                    "account_id": account_id,
                    "summary": summary,
                    "positions": pos_dicts,
                })
            self.last_error = None
            logger.info(
                "[monitor_ib] AccountIbClient.fetch_accounts_snapshot: %s accounts",
                len(accounts_list),
            )
            return accounts_list
        except Exception as e:
            self.last_error = str(e)
            logger.warning(
                "[monitor_ib] AccountIbClient.fetch_accounts_snapshot failed: %s",
                e,
                exc_info=True,
            )
            return []


class MarketIbClient(BaseMonitorIbClient):
    """Monitor-side market client: historical bars and other market data."""

    # Simple in-memory time window cache to avoid frequent duplicate IB requests.
    def __init__(self, host: str, port: int, client_id: int, *, name: str, window_seconds: int = 120) -> None:
        super().__init__(host, port, client_id, name=name)
        self._window_seconds = int(window_seconds)
        self._last_fetch_meta: Dict[Tuple[str, str, str], float] = {}

    def mark_fetched(self, symbol: str, period: str, duration: str) -> None:
        key = (symbol.strip(), period.strip(), duration.strip())
        self._last_fetch_meta[key] = time.time()

    def recently_fetched(self, symbol: str, period: str, duration: str) -> bool:
        key = (symbol.strip(), period.strip(), duration.strip())
        ts = self._last_fetch_meta.get(key)
        if ts is None:
            return False
        return (time.time() - ts) < self._window_seconds

    async def fetch_bars(self, symbol: str, period: str, duration: str) -> List[Dict[str, Any]]:
        return await self._run_on_client_loop(self._fetch_bars_impl(symbol, period, duration))

    async def _fetch_bars_impl(self, symbol: str, period: str, duration: str) -> List[Dict[str, Any]]:
        """Fetch historical bars from IB for given symbol/period/duration."""
        await self._ensure_connected_impl()
        assert self.connector is not None  # for type checkers
        try:
            raw = await self.connector.get_historical_bars_async(
                symbol, period=period, duration_str=duration
            )
            self.mark_fetched(symbol, period, duration)
            self.last_error = None
            return raw
        except Exception as e:
            self.last_error = str(e)
            logger.warning(
                "[monitor_ib] MarketIbClient.fetch_bars failed: %s", e, exc_info=True
            )
            return []

    async def fetch_bars_range(
        self,
        symbol: str,
        period: str,
        *,
        start_ts: Optional[float] = None,
        end_ts: Optional[float] = None,
        interval_sec: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        return await self._run_on_client_loop(
            self._fetch_bars_range_impl(
                symbol,
                period,
                start_ts=start_ts,
                end_ts=end_ts,
                interval_sec=interval_sec,
            )
        )

    async def _fetch_bars_range_impl(
        self,
        symbol: str,
        period: str,
        *,
        start_ts: Optional[float] = None,
        end_ts: Optional[float] = None,
        interval_sec: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        """按时间范围分段拉取历史 K 线（供补全历史使用），复用当前 Market 连接的 client_id。"""
        await self._ensure_connected_impl()
        assert self.connector is not None
        try:
            raw = await self.connector.get_historical_bars_range(
                symbol=symbol,
                period=period,
                start_ts=start_ts,
                end_ts=end_ts,
                interval_sec=interval_sec,
            )
            self.last_error = None
            return raw
        except IBConnectionDroppedError as e:
            self.last_error = str(e)
            logger.warning(
                "[monitor_ib] MarketIbClient.fetch_bars_range connection dropped: %s",
                e,
                exc_info=True,
            )
            try:
                await self.disconnect()
            except Exception:
                pass
            raise
        except Exception as e:
            self.last_error = str(e)
            logger.warning(
                "[monitor_ib] MarketIbClient.fetch_bars_range failed: %s", e, exc_info=True
            )
            raise

    async def fetch_option_expirations(self, symbol: str) -> Dict[str, Any]:
        """Fetch option expirations and strikes for underlying symbol (IB reqSecDefOptParams).
        Returns { expirations: List[str], strikes: List[float], error: Optional[str] }."""
        return await self._run_on_client_loop(self._fetch_option_expirations_impl(symbol))

    async def _fetch_option_expirations_impl(self, symbol: str) -> Dict[str, Any]:
        await self._ensure_connected_impl()
        assert self.connector is not None
        out: Dict[str, Any] = {"expirations": [], "strikes": []}
        try:
            expirations, strikes = await self.connector.get_sec_def_opt_params_async(
                symbol.strip(), "", "STK", 0
            )
            out["expirations"] = expirations
            out["strikes"] = strikes
            self.last_error = None
        except IBConnectionDroppedError as e:
            self.last_error = str(e)
            out["error"] = str(e)
            logger.warning(
                "[monitor_ib] MarketIbClient.fetch_option_expirations connection dropped: %s",
                e,
                exc_info=True,
            )
        except Exception as e:
            self.last_error = str(e)
            out["error"] = str(e)
            logger.warning(
                "[monitor_ib] MarketIbClient.fetch_option_expirations failed: %s",
                e,
                exc_info=True,
            )
        return out

    async def fetch_underlying_price(self, symbol: str) -> Optional[float]:
        """Get mid/last price for underlying stock (for ATM strike selection)."""
        await self._ensure_connected_impl()
        if self.connector is None:
            return None
        try:
            return await self.connector.get_underlying_price(symbol.strip())
        except Exception as e:
            logger.debug("[monitor_ib] fetch_underlying_price %s: %s", symbol, e)
            return None

    async def fetch_option_quote(
        self,
        symbol: str,
        expiry: str,
        strike: float,
        right: str,
    ) -> Optional[Dict[str, Optional[float]]]:
        """Get one option quote (bid/ask/last/mid) and cancel subscription immediately."""
        await self._ensure_connected_impl()
        if self.connector is None:
            return None
        try:
            return await self.connector.get_option_quote_one_shot(
                symbol.strip(), expiry.strip(), float(strike), (right or "C").upper()
            )
        except Exception as e:
            logger.debug(
                "[monitor_ib] fetch_option_quote %s %s %s %s: %s",
                symbol,
                expiry,
                strike,
                right,
                e,
            )
            return None

    async def fetch_option_snapshot(
        self,
        symbol: str,
        expiration: str,
        strikes: List[float],
        max_contracts: int = 20,
        pacing_sec: float = 0.35,
    ) -> Tuple[List[Dict[str, Any]], Optional[float]]:
        """Fetch option quotes for (symbol, expiration) for a subset of strikes (C and P each).
        Returns (rows with strike, right, bid, ask, last, mid; underlying_price if available).
        When strikes is non-empty, skips fetch_underlying_price to avoid ~2min first-call delay."""
        await self._ensure_connected_impl()
        rows: List[Dict[str, Any]] = []
        underlying_price: Optional[float] = None
        if not strikes:
            # Need spot to compute strike list; fetch once (can be slow on first call).
            try:
                underlying_price = await self.fetch_underlying_price(symbol)
            except Exception:
                pass
        # Filter out invalid strikes (e.g. 0.5, 1.0 from wrong chain); US equity strikes are dollar amounts.
        if underlying_price is not None and underlying_price > 0:
            min_s = max(0.5, underlying_price * 0.01)
            max_s = underlying_price * 2.5
            strikes = [s for s in strikes if min_s <= s <= max_s]
        else:
            strikes = [s for s in strikes if s >= 5.0]
        max_strikes = min(max_contracts // 2, len(strikes)) if strikes else 0
        if max_strikes <= 0:
            return rows, underlying_price
        if underlying_price is not None and strikes:
            sorted_strikes = sorted(strikes, key=lambda s: abs(s - underlying_price))
            selected_strikes = sorted_strikes[:max_strikes]
        else:
            selected_strikes = strikes[:max_strikes]
        for strike in selected_strikes:
            for right in ("C", "P"):
                try:
                    quote = await self.fetch_option_quote(symbol, expiration, strike, right)
                    row: Dict[str, Any] = {
                        "strike": strike,
                        "right": right,
                        "bid": None,
                        "ask": None,
                        "last": None,
                        "mid": None,
                    }
                    if quote:
                        row["bid"] = quote.get("bid")
                        row["ask"] = quote.get("ask")
                        row["last"] = quote.get("last")
                        row["mid"] = quote.get("mid")
                    rows.append(row)
                except Exception as e:
                    logger.warning(
                        "[monitor_ib] fetch_option_snapshot quote %s %s %s %s: %s",
                        symbol,
                        expiration,
                        strike,
                        right,
                        e,
                    )
                    rows.append(
                        {
                            "strike": strike,
                            "right": right,
                            "bid": None,
                            "ask": None,
                            "last": None,
                            "mid": None,
                        }
                    )
                await asyncio.sleep(pacing_sec)
        return rows, underlying_price


class OperatorIbClient(MarketIbClient):
    """Single IB API client for IB Operator: account + market RPC on one client_id."""

    async def fetch_executions(self, days: int) -> List[Dict[str, Any]]:
        return await self._run_on_client_loop(self._fetch_executions_impl(days))

    async def _fetch_executions_impl(self, days: int) -> List[Dict[str, Any]]:
        await self._ensure_connected_impl()
        assert self.connector is not None
        try:
            account_ids = self.connector.get_managed_accounts()
            if not account_ids:
                account_ids = [""]
            all_execs: List[Dict[str, Any]] = []
            for acc in account_ids:
                exec_list = await self.connector.get_executions_async(
                    account=acc or None,
                    since_days=days,
                )
                if exec_list:
                    all_execs.extend(exec_list)
            logger.info(
                "[monitor_ib] OperatorIbClient.fetch_executions: %s executions over %s days",
                len(all_execs),
                days,
            )
            self.last_error = None
            return all_execs
        except Exception as e:
            self.last_error = str(e)
            logger.warning(
                "[monitor_ib] OperatorIbClient.fetch_executions failed: %s", e, exc_info=True
            )
            return []

    async def fetch_accounts_snapshot(self) -> List[Dict[str, Any]]:
        return await self._run_on_client_loop(self._fetch_accounts_snapshot_impl())

    async def _fetch_accounts_snapshot_impl(self) -> List[Dict[str, Any]]:
        await self._ensure_connected_impl()
        assert self.connector is not None
        try:
            account_ids = self.connector.get_managed_accounts()
            if not account_ids:
                logger.warning(
                    "[monitor_ib] OperatorIbClient.fetch_accounts_snapshot: get_managed_accounts returned 0"
                )
                return []
            all_positions = await self.connector.get_positions(account=None)
            accounts_list: List[Dict[str, Any]] = []
            for account_id in account_ids:
                values = await self.connector.get_account_summary(account=account_id)
                summary: Dict[str, Any] = {}
                for v in values:
                    if getattr(v, "tag", None) and getattr(v, "value", None) is not None:
                        summary[v.tag] = v.value
                if account_id:
                    summary["account"] = account_id
                acct_positions = [
                    p for p in all_positions if getattr(p, "account", None) == account_id
                ]
                pos_dicts = [
                    self.connector.position_to_dict(p) for p in acct_positions
                ]
                accounts_list.append({
                    "account_id": account_id,
                    "summary": summary,
                    "positions": pos_dicts,
                })
            self.last_error = None
            logger.info(
                "[monitor_ib] OperatorIbClient.fetch_accounts_snapshot: %s accounts",
                len(accounts_list),
            )
            return accounts_list
        except Exception as e:
            self.last_error = str(e)
            logger.warning(
                "[monitor_ib] OperatorIbClient.fetch_accounts_snapshot failed: %s",
                e,
                exc_info=True,
            )
            return []

