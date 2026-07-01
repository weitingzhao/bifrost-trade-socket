"""IB connector: connect, positions, ticker; optional subscriptions.

Ported from bifrost-trader-engine/src/connector/ib.py.
No src.* imports — depends only on ib_insync and stdlib.
"""

import asyncio
import logging
from typing import Any, Callable, Dict, List, Optional, Tuple

from ib_insync import (
    IB,
    Stock,
    Trade,
    Fill,
    Position,
    Ticker,
    AccountValue,
    Option,
    ExecutionFilter,
)

logger = logging.getLogger(__name__)


class IBConnectionDroppedError(ConnectionError):
    """Raised when an existing IB connection drops during an in-flight request."""


class IBConnector:
    """Minimal IB connector for gamma scalping daemon."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 4001,
        client_id: int = 1,
        connect_timeout: float = 60.0,
    ):
        self.host = host
        self.port = port
        self.client_id = client_id
        self.connect_timeout = connect_timeout
        self.ib = IB()
        self._connected = False
        self._commission_registered = False
        self._disconnect_event_registered = False
        self._stock_contract: Optional[Stock] = None
        self._tickers: Dict[str, Ticker] = {}
        self._option_tickers: Dict[str, Ticker] = {}
        self._commission_report_callback: Optional[
            Callable[[str, Optional[float], Optional[float], Optional[str], Optional[float], Optional[int]], None]
        ] = None

    def set_commission_report_callback(
        self,
        callback: Optional[
            Callable[[str, Optional[float], Optional[float], Optional[str], Optional[float], Optional[int]], None]
        ],
    ) -> None:
        self._commission_report_callback = callback

    def _on_commission_report(self, trade: Any, fill: Any, report: Any) -> None:
        if not self._commission_report_callback or not fill or not report:
            return
        ex = getattr(fill, "execution", None)
        exec_id = getattr(ex, "execId", None) if ex else None
        if not exec_id:
            return
        commission = getattr(report, "commission", None)
        realized_pnl = getattr(report, "realizedPNL", None)
        currency = getattr(report, "currency", None)
        yield_ = getattr(report, "yield_", None)
        yield_redemption_date = getattr(report, "yieldRedemptionDate", None)
        if yield_redemption_date is not None:
            try:
                yield_redemption_date = int(yield_redemption_date)
            except (TypeError, ValueError):
                yield_redemption_date = None
        try:
            self._commission_report_callback(exec_id, commission, realized_pnl, currency, yield_, yield_redemption_date)
        except Exception as e:
            logger.warning("commission_report_callback failed: exec_id=%r %s", exec_id, e)

    @property
    def is_connected(self) -> bool:
        return self._connected and self.ib.isConnected()

    def _stock(self, symbol: str, exchange: str = "SMART") -> Stock:
        return Stock(symbol, exchange, "USD")

    _CONNECT_ATTEMPT_TIMEOUT = 15.0

    @staticmethod
    def _is_connection_refused(exc: Exception) -> bool:
        en = getattr(exc, "errno", None)
        if en in (111, 61, 10061):
            return True
        msg = (getattr(exc, "message", None) or str(exc)).lower()
        return "refus" in msg or "connection refused" in msg

    @staticmethod
    def _is_connection_dropped(exc: Exception) -> bool:
        if isinstance(exc, (ConnectionError, IBConnectionDroppedError)):
            return True
        if getattr(exc, "errno", None) in (54, 104):
            return True
        msg = (getattr(exc, "message", None) or str(exc)).lower()
        return any(token in msg for token in (
            "socket disconnect",
            "connection reset by peer",
            "peer closed connection",
            "not connected",
            "connection closed",
            "disconnected",
        ))

    def _on_ib_disconnected(self) -> None:
        was = self._connected
        self._connected = False
        if was:
            logger.warning("IB disconnectedEvent fired — _connected cleared (was True)")

    def _mark_connection_dropped(self, reason: str, exc: Exception) -> None:
        logger.warning("IB connection dropped (%s): %s", reason, exc)
        try:
            if self.ib.isConnected():
                self.ib.disconnect()
        except Exception:
            pass
        self._connected = False

    async def connect(
        self,
        max_attempts: Optional[int] = None,
        bars_only: bool = False,
        max_port_steps: int = 1,
    ) -> bool:
        """Connect to TWS/Gateway.

        When max_attempts is 1: try once with current client_id and return.
        When max_attempts is None or >1: try up to max_attempts (default 10) with client_id, client_id+1, ...
        When max_port_steps > 1: after connection refused, try self.port+1, ... up to max_port_steps ports.
        When bars_only is True, do not register commissionReportEvent.
        """
        if self.is_connected:
            return True
        base_id = self.client_id
        base_port = int(self.port)
        limit = max_attempts if max_attempts is not None else 10
        last_exc: Optional[Exception] = None
        attempt_timeout = min(self.connect_timeout, self._CONNECT_ATTEMPT_TIMEOUT)
        wait_secs = int(attempt_timeout) + 5
        steps = max(1, min(20, int(max_port_steps)))

        for port_step in range(steps):
            try_port = base_port + port_step
            for attempt in range(limit):
                try_id = base_id + attempt
                logger.info(
                    "IB connect port=%s (%s/%s) clientId=%s (%s/%s): up to %s–%ss%s",
                    try_port,
                    port_step + 1,
                    steps,
                    try_id,
                    attempt + 1,
                    limit,
                    int(attempt_timeout),
                    wait_secs,
                    (
                        " (single attempt per heartbeat)"
                        if limit == 1
                        else "; if client_id in use will retry with next ID"
                    ),
                )
                try:
                    logger.debug(
                        "Connecting to IB %s:%s clientId=%s timeout=%.0fs",
                        self.host,
                        try_port,
                        try_id,
                        attempt_timeout,
                    )
                    await asyncio.wait_for(
                        self.ib.connectAsync(
                            self.host,
                            try_port,
                            clientId=try_id,
                            timeout=attempt_timeout,
                        ),
                        timeout=attempt_timeout + 5.0,
                    )
                    self.port = try_port
                    self.client_id = try_id
                    self._connected = True
                    if not self._disconnect_event_registered:
                        self.ib.disconnectedEvent += self._on_ib_disconnected
                        self._disconnect_event_registered = True
                    if try_port != base_port:
                        logger.info(
                            "Connected to IB %s:%s clientId=%s (base port was %s)",
                            self.host,
                            try_port,
                            try_id,
                            base_port,
                        )
                    elif try_id != base_id:
                        logger.info(
                            "Connected to IB %s:%s clientId=%s (base %s was in use)",
                            self.host,
                            try_port,
                            try_id,
                            base_id,
                        )
                    else:
                        logger.info(
                            "Connected to IB %s:%s clientId=%s",
                            self.host,
                            try_port,
                            self.client_id,
                        )
                    if not bars_only:
                        self.ib.commissionReportEvent += self._on_commission_report
                        self._commission_registered = True
                    return True
                except Exception as e:
                    last_exc = e
                    if self.ib.isConnected():
                        try:
                            self.ib.disconnect()
                        except Exception:
                            pass
                    if self._is_connection_refused(e):
                        if port_step < steps - 1:
                            logger.warning(
                                "IB connection refused on %s:%s (%s); trying port %s",
                                self.host,
                                try_port,
                                e,
                                try_port + 1,
                            )
                            break
                        logger.error(
                            "IB connection refused on %s:%s (%s)",
                            self.host,
                            try_port,
                            e,
                        )
                        self._connected = False
                        return False
                    if attempt < limit - 1:
                        logger.warning(
                            "IB clientId=%s failed (%s), retrying with clientId=%s (next attempt may take up to %ss)",
                            try_id,
                            e,
                            try_id + 1,
                            wait_secs,
                        )
                    else:
                        if (
                            port_step < steps - 1
                            and last_exc is not None
                            and self._is_connection_refused(last_exc)
                        ):
                            logger.warning(
                                "IB all client ids failed on %s:%s (%s); trying port %s",
                                self.host,
                                try_port,
                                last_exc,
                                try_port + 1,
                            )
                            break
                        if limit == 1:
                            logger.debug(
                                "IB connect attempt failed (will retry on next heartbeat): %s",
                                last_exc,
                            )
                        else:
                            logger.error(
                                "IB connect failed after %s port step(s) x %s client ids: %s",
                                steps,
                                limit,
                                last_exc,
                            )
                        self._connected = False
                        return False
        self._connected = False
        if last_exc:
            logger.error("IB connect failed after %s port step(s): %s", steps, last_exc)
        return False

    async def disconnect(self) -> None:
        """Disconnect from IB."""
        if not self._connected:
            return
        for ck, ticker in list(self._option_tickers.items()):
            try:
                self.ib.cancelMktData(ticker)
            except Exception as e:
                logger.debug("disconnect cancelMktData option %s: %s", ck, e)
        self._option_tickers.clear()
        self._tickers.clear()
        if self._commission_registered:
            try:
                self.ib.commissionReportEvent -= self._on_commission_report
            except Exception:
                pass
            self._commission_registered = False
        try:
            self.ib.disconnect()
        except Exception as e:
            logger.error("IB disconnect error: %s", e)
        self._connected = False
        logger.info("Disconnected from IB")

    def get_managed_accounts(self) -> List[str]:
        """Return list of managed account IDs. Empty when not connected."""
        if not self.is_connected:
            return []
        try:
            raw = self.ib.managedAccounts()
            if not raw:
                return []
            if isinstance(raw, str):
                parts = raw.split(",")
            else:
                parts = [str(s) for s in raw]
            return [s.strip() for s in parts if s.strip()]
        except Exception as e:
            logger.warning("get_managed_accounts: %s", e, exc_info=True)
            return []

    async def get_account_summary(
        self, account: Optional[str] = None
    ) -> List[AccountValue]:
        """Request and return account summary (NetLiquidation, TotalCashValue, BuyingPower, etc.)."""
        if not self.is_connected:
            return []
        try:
            values = await self.ib.accountSummaryAsync(account or "")
            return list(values) if values else []
        except Exception as e:
            logger.warning("get_account_summary: %s", e)
            return []

    @staticmethod
    def position_to_dict(pos: Position) -> Dict[str, Any]:
        """Convert IB Position to a JSON-serializable dict."""
        c = pos.contract
        sec_type = getattr(c, "secType", "") or ""
        out: Dict[str, Any] = {
            "account": pos.account,
            "symbol": getattr(c, "symbol", "") or "",
            "secType": sec_type,
            "exchange": getattr(c, "exchange", "") or "",
            "currency": getattr(c, "currency", "") or "",
            "position": float(pos.position),
            "avgCost": float(pos.avgCost) if pos.avgCost is not None else None,
        }
        if sec_type == "OPT":
            out["lastTradeDateOrContractMonth"] = (
                getattr(c, "lastTradeDateOrContractMonth", None) or ""
            )
            out["strike"] = getattr(c, "strike", None)
            out["right"] = getattr(c, "right", None) or ""
            out["multiplier"] = getattr(c, "multiplier", None)
        return out

    async def get_positions(self, account: Optional[str] = None) -> List[Position]:
        """Return list of IB Position objects."""
        if not self.is_connected:
            await self.connect()
        await self.ib.reqPositionsAsync()
        positions = self.ib.positions(account)
        return list(positions)

    def get_positions_sync(self) -> List[Position]:
        """Synchronous positions (for use inside IB callbacks)."""
        return list(self.ib.positions())

    async def get_underlying_price(self, symbol: str) -> Optional[float]:
        """Get mid price for underlying stock."""
        if not self.is_connected:
            await self.connect()
        stock = self._stock(symbol)
        try:
            await self.ib.qualifyContractsAsync(stock)
            ticker = self.ib.reqMktData(stock, "", False, False)
            await asyncio.sleep(0.5)
            mid = (
                (ticker.bid + ticker.ask) / 2.0
                if (ticker.bid and ticker.ask)
                else (ticker.last if ticker.last else None)
            )
            return float(mid) if mid is not None else None
        except Exception as e:
            logger.error("get_underlying_price %s: %s", symbol, e)
        return None

    async def get_instrument_price(
        self,
        symbol: str,
        sec_type: str,
        expiry: Optional[str] = None,
        strike: Optional[float] = None,
        right: Optional[str] = None,
        exchange: str = "SMART",
        currency: str = "USD",
    ) -> Optional[Dict[str, Optional[float]]]:
        """Get price for a generic instrument (stock/option)."""
        if not self.is_connected:
            await self.connect()
        sec = (sec_type or "").upper()
        if not symbol:
            return None
        contract = None
        try:
            if sec == "OPT":
                exp = (expiry or "").strip()
                if not exp or strike is None or right is None:
                    return None
                rt = str(right).upper()
                contract = Option(
                    symbol=symbol,
                    lastTradeDateOrContractMonth=exp,
                    strike=float(strike),
                    right=rt,
                    exchange=exchange,
                    currency=currency,
                )
            else:
                contract = self._stock(symbol, exchange)
            await self.ib.qualifyContractsAsync(contract)
            ticker = self.ib.reqMktData(contract, "", False, False)
            bid = ask = last = mid = None
            for _ in range(3):
                await asyncio.sleep(0.5)
                tbid = getattr(ticker, "bid", None)
                task = getattr(ticker, "ask", None)
                tlast = getattr(ticker, "last", None)
                try:
                    if tbid is not None:
                        fb = float(tbid)
                        if fb > 0:
                            bid = fb
                    if task is not None:
                        fa = float(task)
                        if fa > 0:
                            ask = fa
                    if tlast is not None:
                        fl = float(tlast)
                        if fl > 0:
                            last = fl
                except (TypeError, ValueError):
                    pass
                if bid is not None and ask is not None:
                    mid = (bid + ask) / 2.0
                elif last is not None:
                    mid = last
                if bid is not None or ask is not None or last is not None or mid is not None:
                    break
            if bid is None and ask is None and last is None and mid is None:
                return None
            return {"bid": bid, "ask": ask, "last": last, "mid": mid}
        except Exception as e:
            logger.error("get_instrument_price %s %s: %s", sec_type, symbol, e)
            return None

    async def get_option_quote_one_shot(
        self,
        symbol: str,
        expiry: str,
        strike: float,
        right: str,
        exchange: str = "SMART",
        currency: str = "USD",
    ) -> Optional[Dict[str, Optional[float]]]:
        """Get one option quote and cancel subscription immediately."""
        if not self.is_connected:
            await self.connect()
        exp = (expiry or "").strip()
        rt = (right or "").upper()
        if not symbol or not exp or rt not in ("C", "P"):
            return None
        contract = Option(
            symbol=symbol,
            lastTradeDateOrContractMonth=exp,
            strike=float(strike),
            right=rt,
            exchange=exchange,
            currency=currency,
        )

        async def _read_ticker(ticker: Any) -> tuple:
            bid = ask = last = mid = None
            for _ in range(4):
                await asyncio.sleep(0.5)
                tbid = getattr(ticker, "bid", None)
                task = getattr(ticker, "ask", None)
                tlast = getattr(ticker, "last", None)
                try:
                    if tbid is not None:
                        fb = float(tbid)
                        if fb > 0:
                            bid = fb
                    if task is not None:
                        fa = float(task)
                        if fa > 0:
                            ask = fa
                    if tlast is not None:
                        fl = float(tlast)
                        if fl > 0:
                            last = fl
                except (TypeError, ValueError):
                    pass
                if bid is not None and ask is not None:
                    mid = (bid + ask) / 2.0
                elif last is not None:
                    mid = last
                if bid is not None or ask is not None or last is not None or mid is not None:
                    break
            return bid, ask, last, mid

        ticker = None
        try:
            ticker = self.ib.reqMktData(contract, "", False, False)
            bid, ask, last, mid = await _read_ticker(ticker)
            if bid is not None or ask is not None or last is not None or mid is not None:
                try:
                    self.ib.cancelMktData(ticker)
                except Exception as cancel_err:
                    logger.debug("cancelMktData after option quote: %s", cancel_err)
                ticker = None
                return {"bid": bid, "ask": ask, "last": last, "mid": mid}
        except Exception:
            if ticker is not None:
                try:
                    self.ib.cancelMktData(ticker)
                except Exception:
                    pass

        try:
            await self.ib.qualifyContractsAsync(contract)
            ticker = self.ib.reqMktData(contract, "", False, False)
            bid, ask, last, mid = await _read_ticker(ticker)
            try:
                self.ib.cancelMktData(ticker)
            except Exception as cancel_err:
                logger.debug("cancelMktData after option quote (fallback): %s", cancel_err)
            if bid is None and ask is None and last is None and mid is None:
                return None
            return {"bid": bid, "ask": ask, "last": last, "mid": mid}
        except Exception as e:
            logger.error("get_option_quote_one_shot %s %s %s %s: %s", symbol, exp, strike, rt, e)
            return None

    async def get_sec_def_opt_params_async(
        self,
        underlying_symbol: str,
        fut_fop_exchange: str = "",
        underlying_sec_type: str = "STK",
        underlying_con_id: int = 0,
    ) -> Tuple[List[str], List[float]]:
        """Request option expirations and strikes for an underlying (reqSecDefOptParams)."""
        sym = underlying_symbol.strip()
        con_id = underlying_con_id
        if (not con_id) and underlying_sec_type.upper() == "STK":
            stock = self._stock(sym)
            try:
                await self.ib.qualifyContractsAsync(stock)
                con_id = int(getattr(stock, "conId", 0) or 0)
            except Exception as e:
                logger.warning("get_sec_def_opt_params_async qualify %s: %s", sym, e)
        timeout_sec = 15.0
        try:
            chains = await asyncio.wait_for(
                self.ib.reqSecDefOptParamsAsync(
                    sym,
                    fut_fop_exchange,
                    underlying_sec_type,
                    con_id,
                ),
                timeout=timeout_sec,
            )
        except asyncio.TimeoutError:
            logger.warning("get_sec_def_opt_params_async %s: timeout after %ss", underlying_symbol, timeout_sec)
            raise
        expirations_set: set = set()
        strikes_set: set = set()
        for chain in chains or []:
            for e in getattr(chain, "expirations", []) or []:
                expirations_set.add(str(e).strip())
            for s in getattr(chain, "strikes", []) or []:
                try:
                    strikes_set.add(float(s))
                except (TypeError, ValueError):
                    pass
        return (sorted(expirations_set), sorted(strikes_set))

    async def subscribe_ticker(
        self,
        symbol: str,
        on_update: Callable[[Ticker], None],
    ) -> Optional[Ticker]:
        """Subscribe to live ticker; on_update called on each tick."""
        if not self.is_connected:
            logger.warning("subscribe_ticker: not connected")
            return None
        stock = self._stock(symbol)
        try:
            await self.ib.qualifyContractsAsync(stock)
            ticker = self.ib.reqMktData(stock, "", False, False)
            ticker.updateEvent += lambda t: on_update(t)
            self._stock_contract = stock
            return ticker
        except Exception as e:
            logger.error("subscribe_ticker %s: %s", symbol, e)
            return None

    async def subscribe_tickers(
        self,
        symbols: List[str],
        on_update: Callable[[str, Ticker], None],
    ) -> Dict[str, Ticker]:
        """Subscribe to live tickers for multiple symbols (STK). on_update(symbol, ticker) on each tick."""
        out: Dict[str, Ticker] = {}
        if not self.is_connected:
            logger.warning("subscribe_tickers: not connected")
            return out
        seen = set()
        for symbol in symbols:
            s = (symbol or "").strip()
            if not s or s in seen:
                continue
            seen.add(s)
            stock = self._stock(s)
            try:
                await self.ib.qualifyContractsAsync(stock)
                ticker = self.ib.reqMktData(stock, "", False, False)

                def _make_cb(sym: str) -> Callable[[Ticker], None]:
                    def cb(t: Ticker) -> None:
                        on_update(sym, t)
                    return cb

                ticker.updateEvent += _make_cb(s)
                self._tickers[s] = ticker
                out[s] = ticker
                if self._stock_contract is None:
                    self._stock_contract = stock
            except Exception as e:
                logger.error("subscribe_tickers %s: %s", s, e)
        return out

    def get_subscribed_ticker_symbols(self) -> List[str]:
        """Return list of symbols currently subscribed for market data."""
        return list(self._tickers.keys())

    def unsubscribe_ticker(self, symbol: str) -> None:
        """Cancel market data for one symbol and remove from _tickers."""
        s = (symbol or "").strip()
        if not s or s not in self._tickers:
            return
        ticker = self._tickers[s]
        try:
            self.ib.cancelMktData(ticker)
        except Exception as e:
            logger.warning("unsubscribe_ticker %s: %s", s, e)
        finally:
            self._tickers.pop(s, None)

    async def subscribe_option_ticker(
        self,
        contract_key: str,
        symbol: str,
        expiry: str,
        strike: float,
        right: str,
        on_update: Callable[[str, Ticker], None],
        exchange: str = "SMART",
        currency: str = "USD",
    ) -> Optional[Ticker]:
        """Subscribe to live ticker for one option contract. on_update(contract_key, ticker) on each tick."""
        if not self.is_connected:
            logger.warning("subscribe_option_ticker: not connected")
            return None
        exp = (expiry or "").strip()
        rt = (right or "C").upper()
        if rt not in ("C", "P"):
            return None
        if not symbol or not exp or strike is None:
            return None
        contract = Option(
            symbol=symbol.strip(),
            lastTradeDateOrContractMonth=exp,
            strike=float(strike),
            right=rt,
            exchange=exchange,
            currency=currency,
        )
        try:
            ticker = self.ib.reqMktData(contract, "", False, False)
            ticker.updateEvent += lambda t: on_update(contract_key, t)
            self._option_tickers[contract_key] = ticker
            return ticker
        except Exception as e:
            logger.debug("subscribe_option_ticker (no qualify) %s: %s", contract_key, e)
        try:
            await self.ib.qualifyContractsAsync(contract)
            ticker = self.ib.reqMktData(contract, "", False, False)
            ticker.updateEvent += lambda t: on_update(contract_key, t)
            self._option_tickers[contract_key] = ticker
            return ticker
        except Exception as e:
            logger.error("subscribe_option_ticker %s: %s", contract_key, e)
            return None

    async def subscribe_option_tickers(
        self,
        contracts: List[Dict[str, Any]],
        on_update: Callable[[str, Ticker], None],
    ) -> Dict[str, Ticker]:
        """Subscribe to live tickers for multiple option contracts. on_update(contract_key, ticker) on each tick."""
        out: Dict[str, Ticker] = {}
        if not self.is_connected:
            logger.warning("subscribe_option_tickers: not connected")
            return out
        seen: set = set()
        for c in contracts:
            ck = (c.get("contract_key") or "").strip()
            if not ck or ck in seen:
                continue
            seen.add(ck)
            symbol = (c.get("symbol") or "").strip()
            expiry = (c.get("expiry") or "").strip()
            strike = c.get("strike")
            right = (c.get("option_right") or "C").strip().upper() or "C"
            if not symbol or not expiry or strike is None:
                continue
            ticker = await self.subscribe_option_ticker(
                ck, symbol, expiry, float(strike), right, on_update
            )
            if ticker is not None:
                out[ck] = ticker
        return out

    def unsubscribe_option_ticker(self, contract_key: str) -> None:
        """Cancel market data for one option contract and remove from _option_tickers."""
        ck = (contract_key or "").strip()
        if not ck or ck not in self._option_tickers:
            return
        ticker = self._option_tickers[ck]
        try:
            self.ib.cancelMktData(ticker)
        except Exception as e:
            logger.warning("unsubscribe_option_ticker %s: %s", ck, e)
        finally:
            self._option_tickers.pop(ck, None)

    def get_subscribed_option_contract_keys(self) -> List[str]:
        """Return list of contract_keys currently subscribed for option market data."""
        return list(self._option_tickers.keys())

    def subscribe_positions(self, on_update: Callable[[], None]) -> None:
        """Subscribe to position updates; on_update() called when positions change."""
        if not self.is_connected:
            return
        self.ib.positionEvent += lambda _: on_update()

    def subscribe_fills(self, on_fill: Callable[[Trade, Any], None]) -> None:
        """Subscribe to fill/trade updates. Callback receives (trade, fill)."""
        if not self.is_connected:
            return
        self.ib.execDetailsEvent += lambda trade, fill: on_fill(trade, fill)

    def subscribe_order_status(self, on_status: Callable[[Trade], None]) -> None:
        """Subscribe to order status changes (Submitted, Filled, Cancelled)."""
        if not self.is_connected:
            return
        self.ib.orderStatusEvent += lambda trade: on_status(trade)

    def subscribe_open_order(self, on_open: Callable[[Trade], None]) -> None:
        """Subscribe to new open orders."""
        if not self.is_connected:
            return
        self.ib.openOrderEvent += lambda trade: on_open(trade)

    def _trade_to_open_order_dict(self, trade: Trade) -> Dict[str, Any]:
        contract = getattr(trade, "contract", None)
        order = getattr(trade, "order", None)
        status = getattr(trade, "orderStatus", None)
        order_id = getattr(order, "orderId", None) if order else None
        perm_id = getattr(order, "permId", None) if order else None
        account_id = getattr(order, "account", None) or getattr(trade, "account", None)
        if account_id is None and order is not None:
            account_id = getattr(order, "account", None)
        symbol = getattr(contract, "symbol", "") or "" if contract else ""
        sec_type = getattr(contract, "secType", "") or "" if contract else ""
        action = getattr(order, "action", "") or "" if order else ""
        total_quantity = getattr(order, "totalQuantity", 0) or 0
        if status is not None:
            filled = getattr(status, "filled", 0) or 0
            remaining = getattr(status, "remaining", 0) or 0
            order_status = getattr(status, "status", "") or ""
        else:
            filled = 0
            remaining = total_quantity
            order_status = ""
        limit_price = getattr(order, "lmtPrice", None) if order else None
        if limit_price is not None and (limit_price == 0 or (isinstance(limit_price, float) and limit_price != limit_price)):
            limit_price = None
        contract_key = self._contract_key(contract) if contract else ""
        return {
            "order_id": order_id,
            "perm_id": perm_id,
            "account_id": str(account_id) if account_id is not None else None,
            "symbol": symbol,
            "sec_type": sec_type,
            "action": action,
            "total_quantity": total_quantity,
            "filled": filled,
            "remaining": remaining,
            "limit_price": limit_price,
            "status": order_status,
            "contract_key": contract_key,
        }

    def get_open_orders_snapshot(self) -> List[Dict[str, Any]]:
        """Sync snapshot of current open orders from ib.openTrades()."""
        out: List[Dict[str, Any]] = []
        for trade in self.ib.openTrades() or []:
            if not isinstance(trade, Trade):
                continue
            out.append(self._trade_to_open_order_dict(trade))
        return out

    async def get_open_orders_async(
        self, include_all_from_tws: bool = False
    ) -> List[Dict[str, Any]]:
        """Return current open orders from ib.openTrades()."""
        if not self.is_connected:
            await self.connect()
        if include_all_from_tws:
            await self.ib.reqAllOpenOrdersAsync()
            await self.ib.reqOpenOrdersAsync()
        return self.get_open_orders_snapshot()

    def _exec_side_to_buy_sell(self, side: Optional[str]) -> str:
        if not side:
            return ""
        s = str(side).strip().upper()
        if s in ("BOT", "BUY", "B"):
            return "BUY"
        if s in ("SLD", "SELL", "S"):
            return "SELL"
        return side

    def _contract_key(self, contract: Any) -> str:
        if contract is None:
            return ""
        sym = getattr(contract, "symbol", "") or ""
        st = getattr(contract, "secType", "") or ""
        if st == "OPT":
            exp = getattr(contract, "lastTradeDateOrContractMonth", "") or ""
            strike = getattr(contract, "strike", 0) or 0
            right = getattr(contract, "right", "") or ""
            return f"{sym}|{st}|{exp}|{strike}|{right}"
        return f"{sym}|{st}||||"

    def fill_to_execution_row(
        self,
        fill: Any,
        commission_by_exec_id: Optional[Dict[str, Dict[str, Any]]] = None,
        source: str = "tws_event",
    ) -> Optional[Dict[str, Any]]:
        """Build one execution row dict from a single Fill for DB. Used by subscribe_fills callback."""
        if not isinstance(fill, Fill):
            return None
        ex = getattr(fill, "execution", None)
        contract = getattr(fill, "contract", None)
        comm_report = getattr(fill, "commissionReport", None)
        fill_time = getattr(fill, "time", None) or (ex.time if ex else None)
        exec_id = ex.execId if ex else None
        if not ex:
            return None
        acct = ex.acctNumber if ex else None
        side = self._exec_side_to_buy_sell(ex.side if ex else None)
        shares = ex.shares if ex else None
        price = ex.price if ex else None
        commission = None
        realized_pnl = None
        comm_currency = None
        if comm_report is not None:
            commission = getattr(comm_report, "commission", None)
            realized_pnl = getattr(comm_report, "realizedPNL", None)
            comm_currency = getattr(comm_report, "currency", None)
        if commission is None and exec_id and commission_by_exec_id and exec_id in commission_by_exec_id:
            rec = commission_by_exec_id[exec_id]
            commission = rec.get("commission")
            realized_pnl = realized_pnl if realized_pnl is not None else rec.get("realizedPNL")
            comm_currency = comm_currency or rec.get("currency")
        symbol = ""
        sec_type = ""
        expiry = ""
        strike = None
        option_right = ""
        exchange = ""
        currency = ""
        local_symbol = ""
        con_id = None
        if contract is not None:
            symbol = getattr(contract, "symbol", "") or ""
            sec_type = getattr(contract, "secType", "") or ""
            exchange = getattr(contract, "exchange", "") or ""
            currency = getattr(contract, "currency", "") or ""
            local_symbol = getattr(contract, "localSymbol", "") or ""
            con_id = getattr(contract, "conId", None)
            if sec_type == "OPT":
                expiry = getattr(contract, "lastTradeDateOrContractMonth", "") or ""
                strike = getattr(contract, "strike", None)
                option_right = getattr(contract, "right", "") or ""
        if ex and not exchange:
            exchange = getattr(ex, "exchange", "") or ""
        ts = None
        if fill_time is not None:
            try:
                ts = fill_time.timestamp()
            except Exception:
                pass
        contract_key = self._contract_key(contract)
        raw_extra: Dict[str, Any] = {}
        if ex:
            for attr in ("permId", "clientId", "orderId", "liquidation", "cumQty", "avgPrice", "orderRef", "evRule", "evMultiplier", "modelCode", "lastLiquidity"):
                v = getattr(ex, attr, None)
                if v is not None and v != "" and v != 0:
                    raw_extra[attr] = v
        if comm_report:
            for attr in ("yield_", "yieldRedemptionDate"):
                v = getattr(comm_report, attr, None)
                if v is not None:
                    raw_extra[attr] = v
        yield_val = getattr(comm_report, "yield_", None) if comm_report else None
        yield_redemption = getattr(comm_report, "yieldRedemptionDate", None) if comm_report else None
        if contract and con_id is not None:
            raw_extra["conId"] = con_id
        if local_symbol:
            raw_extra["localSymbol"] = local_symbol
        return {
            "exec_id": exec_id,
            "time": ts,
            "account_id": acct,
            "symbol": symbol,
            "sec_type": sec_type,
            "side": side,
            "quantity": float(shares) if shares is not None else None,
            "price": float(price) if price is not None else None,
            "commission": float(commission) if commission is not None else None,
            "source": source,
            "expiry": expiry or None,
            "strike": float(strike) if strike is not None else None,
            "option_right": option_right or None,
            "exchange": exchange or None,
            "currency": (comm_currency or currency or None),
            "order_id": ex.orderId if ex else None,
            "cum_qty": float(ex.cumQty) if ex and hasattr(ex, "cumQty") and ex.cumQty is not None else None,
            "realized_pnl": float(realized_pnl) if realized_pnl is not None else None,
            "contract_key": contract_key or None,
            "raw_extra": raw_extra if raw_extra else None,
            "yield_": float(yield_val) if yield_val is not None else None,
            "yield_redemption_date": int(yield_redemption) if yield_redemption is not None else None,
        }

    async def get_executions_async(
        self,
        account: Optional[str] = None,
        since_days: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Request from IB and return account executions/fills for DB storage.

        since_days: 1=today (default), 3=last 3 days, 7=last 7 days.
        TWS sends execDetails/execDetailsEnd before commissionReport; we subscribe and wait to merge them.
        """
        if not self.is_connected:
            await self.connect()
        try:
            from datetime import datetime, timedelta, timezone

            time_str = ""
            if since_days is not None and since_days > 0:
                start = datetime.now(timezone.utc) - timedelta(days=since_days - 1)
                time_str = start.strftime("%Y%m%d %H:%M:%S") + " UTC"
            ef = ExecutionFilter(acctCode=account or "", time=time_str)

            commission_by_exec_id: Dict[str, Dict[str, Any]] = {}

            def on_commission_report(trade: Any, fill: Any, report: Any) -> None:
                if fill and getattr(fill, "execution", None) and report:
                    eid = getattr(fill.execution, "execId", None)
                    if eid:
                        commission_by_exec_id[eid] = {
                            "commission": getattr(report, "commission", None),
                            "realizedPNL": getattr(report, "realizedPNL", None),
                            "currency": getattr(report, "currency", None),
                            "yield_": getattr(report, "yield_", None),
                            "yieldRedemptionDate": getattr(report, "yieldRedemptionDate", None),
                        }

            self.ib.commissionReportEvent += on_commission_report
            try:
                fills = await self.ib.reqExecutionsAsync(ef)
                await asyncio.sleep(3.0)
            finally:
                self.ib.commissionReportEvent -= on_commission_report

            out: List[Dict[str, Any]] = []
            seen_exec_ids: set = set()
            for fill in fills or []:
                if not isinstance(fill, Fill):
                    continue
                ex = getattr(fill, "execution", None)
                contract = getattr(fill, "contract", None)
                comm_report = getattr(fill, "commissionReport", None)
                fill_time = getattr(fill, "time", None) or (ex.time if ex else None)
                exec_id = ex.execId if ex else None
                if exec_id and exec_id in seen_exec_ids:
                    continue
                acct = ex.acctNumber if ex else None
                side = self._exec_side_to_buy_sell(ex.side if ex else None)
                shares = ex.shares if ex else None
                price = ex.price if ex else None
                commission = None
                realized_pnl = None
                comm_currency = None
                if comm_report is not None:
                    commission = getattr(comm_report, "commission", None)
                    realized_pnl = getattr(comm_report, "realizedPNL", None)
                    comm_currency = getattr(comm_report, "currency", None)
                if commission is None and exec_id and exec_id in commission_by_exec_id:
                    rec = commission_by_exec_id[exec_id]
                    commission = rec.get("commission")
                    realized_pnl = realized_pnl if realized_pnl is not None else rec.get("realizedPNL")
                    comm_currency = comm_currency or rec.get("currency")
                symbol = ""
                sec_type = ""
                expiry = ""
                strike = None
                option_right = ""
                exchange = ""
                currency = ""
                local_symbol = ""
                con_id = None
                if contract is not None:
                    symbol = getattr(contract, "symbol", "") or ""
                    sec_type = getattr(contract, "secType", "") or ""
                    exchange = getattr(contract, "exchange", "") or ""
                    currency = getattr(contract, "currency", "") or ""
                    local_symbol = getattr(contract, "localSymbol", "") or ""
                    con_id = getattr(contract, "conId", None)
                    if sec_type == "OPT":
                        expiry = getattr(contract, "lastTradeDateOrContractMonth", "") or ""
                        strike = getattr(contract, "strike", None)
                        option_right = getattr(contract, "right", "") or ""
                if ex and not exchange:
                    exchange = getattr(ex, "exchange", "") or ""
                ts = None
                if fill_time is not None:
                    try:
                        ts = fill_time.timestamp()
                    except Exception:
                        pass
                contract_key = self._contract_key(contract)
                raw_extra: Dict[str, Any] = {}
                if ex:
                    for attr in ("permId", "clientId", "orderId", "liquidation", "cumQty", "avgPrice", "orderRef", "evRule", "evMultiplier", "modelCode", "lastLiquidity"):
                        v = getattr(ex, attr, None)
                        if v is not None and v != "" and v != 0:
                            raw_extra[attr] = v
                if comm_report:
                    for attr in ("yield_", "yieldRedemptionDate"):
                        v = getattr(comm_report, attr, None)
                        if v is not None:
                            raw_extra[attr] = v
                yield_val = getattr(comm_report, "yield_", None) if comm_report else None
                yield_redemption = getattr(comm_report, "yieldRedemptionDate", None) if comm_report else None
                if contract and con_id is not None:
                    raw_extra["conId"] = con_id
                if local_symbol:
                    raw_extra["localSymbol"] = local_symbol
                out.append({
                    "exec_id": exec_id,
                    "time": ts,
                    "account_id": acct,
                    "symbol": symbol,
                    "sec_type": sec_type,
                    "side": side,
                    "quantity": float(shares) if shares is not None else None,
                    "price": float(price) if price is not None else None,
                    "commission": float(commission) if commission is not None else None,
                    "source": "tws_client",
                    "expiry": expiry or None,
                    "strike": float(strike) if strike is not None else None,
                    "option_right": option_right or None,
                    "exchange": exchange or None,
                    "currency": (comm_currency or currency or None),
                    "order_id": ex.orderId if ex else None,
                    "cum_qty": float(ex.cumQty) if ex and hasattr(ex, "cumQty") and ex.cumQty is not None else None,
                    "realized_pnl": float(realized_pnl) if realized_pnl is not None else None,
                    "contract_key": contract_key or None,
                    "raw_extra": raw_extra if raw_extra else None,
                    "yield_": float(yield_val) if yield_val is not None else None,
                    "yield_redemption_date": int(yield_redemption) if yield_redemption is not None else None,
                })
                if exec_id:
                    seen_exec_ids.add(exec_id)

            account_filter = (account or "").strip()
            for wfill in self.ib.fills():
                if not isinstance(wfill, Fill):
                    continue
                wex = getattr(wfill, "execution", None)
                if not wex:
                    continue
                weid = getattr(wex, "execId", None)
                if not weid or weid in seen_exec_ids:
                    continue
                wacct = getattr(wex, "acctNumber", None) or ""
                if account_filter and wacct != account_filter:
                    continue
                wcontract = getattr(wfill, "contract", None)
                wcomm = getattr(wfill, "commissionReport", None)
                wtime = getattr(wfill, "time", None) or (wex.time if wex else None)
                wcommission = getattr(wcomm, "commission", None) if wcomm else None
                wrealized = getattr(wcomm, "realizedPNL", None) if wcomm else None
                wcur = getattr(wcomm, "currency", None) if wcomm else None
                wyield_val = getattr(wcomm, "yield_", None) if wcomm else None
                wyield_redemption = getattr(wcomm, "yieldRedemptionDate", None) if wcomm else None
                if wcommission is None and weid in commission_by_exec_id:
                    rec = commission_by_exec_id[weid]
                    wcommission = rec.get("commission")
                    wrealized = wrealized if wrealized is not None else rec.get("realizedPNL")
                    wcur = wcur or rec.get("currency")
                    wyield_val = wyield_val if wyield_val is not None else rec.get("yield_")
                    wyield_redemption = wyield_redemption if wyield_redemption is not None else rec.get("yieldRedemptionDate")
                wsym = getattr(wcontract, "symbol", "") or "" if wcontract else ""
                wst = getattr(wcontract, "secType", "") or "" if wcontract else ""
                wexch = getattr(wcontract, "exchange", "") or "" if wcontract else ""
                wcurr = getattr(wcontract, "currency", "") or "" if wcontract else ""
                if wex and not wexch:
                    wexch = getattr(wex, "exchange", "") or ""
                wts = None
                if wtime is not None:
                    try:
                        wts = wtime.timestamp()
                    except Exception:
                        pass
                out.append({
                    "exec_id": weid,
                    "time": wts,
                    "account_id": wacct,
                    "symbol": wsym,
                    "sec_type": wst,
                    "side": self._exec_side_to_buy_sell(wex.side if wex else None),
                    "quantity": float(wex.shares) if wex and wex.shares is not None else None,
                    "price": float(wex.price) if wex and wex.price is not None else None,
                    "commission": float(wcommission) if wcommission is not None else None,
                    "source": "tws_client",
                    "expiry": getattr(wcontract, "lastTradeDateOrContractMonth", "") or None if wcontract else None,
                    "strike": float(getattr(wcontract, "strike", None)) if wcontract and getattr(wcontract, "strike", None) is not None else None,
                    "option_right": getattr(wcontract, "right", "") or None if wcontract else None,
                    "exchange": wexch or None,
                    "currency": (wcur or wcurr or None),
                    "order_id": wex.orderId if wex else None,
                    "cum_qty": float(wex.cumQty) if wex and getattr(wex, "cumQty", None) is not None else None,
                    "realized_pnl": float(wrealized) if wrealized is not None else None,
                    "contract_key": self._contract_key(wcontract) or None,
                    "raw_extra": None,
                    "yield_": float(wyield_val) if wyield_val is not None else None,
                    "yield_redemption_date": int(wyield_redemption) if wyield_redemption is not None else None,
                })
                seen_exec_ids.add(weid)

            n_second_pass = 0
            try:
                fills_by_id: Dict[str, Any] = {}
                for f in self.ib.fills():
                    ex = getattr(f, "execution", None)
                    if ex:
                        eid = getattr(ex, "execId", None)
                        if eid:
                            fills_by_id[eid] = f
                for row in out:
                    eid = row.get("exec_id")
                    if not eid or row.get("commission") is not None:
                        continue
                    wf = fills_by_id.get(eid)
                    cr = getattr(wf, "commissionReport", None) if wf else None
                    if not cr:
                        continue
                    c = getattr(cr, "commission", None)
                    rp = getattr(cr, "realizedPNL", None)
                    cu = getattr(cr, "currency", None)
                    y_ = getattr(cr, "yield_", None)
                    yr = getattr(cr, "yieldRedemptionDate", None)
                    if c is not None or rp is not None or cu is not None or y_ is not None or yr is not None:
                        row["commission"] = float(c) if c is not None else None
                        row["realized_pnl"] = float(rp) if rp is not None else None
                        row["currency"] = cu or row.get("currency") or None
                        row["yield_"] = float(y_) if y_ is not None else None
                        row["yield_redemption_date"] = int(yr) if yr is not None else None
                        n_second_pass += 1
            except Exception as _e:
                logger.warning("commission from ib.fills() second pass: %s", _e)
            if n_second_pass:
                logger.info("get_executions_async: second pass filled commission for %s rows", n_second_pass)
            logger.info("get_executions_async: got %s fills for account=%r", len(out), account)
            return out
        except Exception as e:
            logger.warning("get_executions_async: %s", e, exc_info=True)
            return []

    _BAR_SIZE_MAP = {
        "1 d": "1 day",
        "1 day": "1 day",
        "1 min": "1 min",
        "5 mins": "5 mins",
        "1 hour": "1 hour",
        "1 h": "1 hour",
    }

    def _convert_ib_bars(self, bars: Any) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for bar in bars or []:
            t = getattr(bar, "date", None)
            ts: Optional[float]
            bar_date_str: Optional[str] = None
            if t is None:
                ts = None
            elif hasattr(t, "timestamp"):
                ts = float(t.timestamp())
                bar_date_str = str(t)[:10]
            else:
                try:
                    from datetime import datetime
                    ts = datetime.fromisoformat(str(t)).timestamp()
                    bar_date_str = str(t)[:10]
                except Exception:
                    ts = None
            if ts is None:
                continue
            entry: Dict[str, Any] = {
                "bar_time": ts,
                "open": float(getattr(bar, "open", 0) or 0),
                "high": float(getattr(bar, "high", 0) or 0),
                "low": float(getattr(bar, "low", 0) or 0),
                "close": float(getattr(bar, "close", 0) or 0),
                "volume": float(getattr(bar, "volume", 0) or 0),
            }
            if bar_date_str:
                entry["bar_date"] = bar_date_str
            out.append(entry)
        return out

    async def get_historical_bars_async(
        self,
        symbol: str,
        period: str = "1 D",
        duration_str: str = "30 D",
    ) -> List[Dict[str, Any]]:
        """Request historical OHLC bars from IB. Returns list of dicts: bar_time (Unix), open, high, low, close, volume."""
        if not self.is_connected:
            await self.connect(bars_only=True)
        if not symbol or not symbol.strip():
            return []
        try:
            stock = self._stock(symbol.strip())
            await self.ib.qualifyContractsAsync(stock)
            bar_setting = self._BAR_SIZE_MAP.get((period or "1 D").strip().lower(), "1 day")
            use_rth = bar_setting != "1 day"
            bars = await self.ib.reqHistoricalDataAsync(
                stock,
                endDateTime="",
                durationStr=duration_str or "30 D",
                barSizeSetting=bar_setting,
                whatToShow="TRADES",
                useRTH=use_rth,
                formatDate=2,
            )
            out = self._convert_ib_bars(bars)
            logger.info("get_historical_bars_async: %s %s %s → %s bars", symbol, period, duration_str, len(out))
            return out
        except Exception as e:
            logger.warning("get_historical_bars_async: %s", e, exc_info=True)
            return []

    async def get_historical_bars_range(
        self,
        symbol: str,
        period: str,
        *,
        start_ts: Optional[float],
        end_ts: Optional[float],
        interval_sec: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        """Request historical OHLC bars over a time range by chunking requests."""
        if not symbol or not symbol.strip():
            return []
        if not self.is_connected:
            await self.connect(bars_only=True)

        from datetime import datetime, timezone
        import time as _time

        sym = symbol.strip()
        per = (period or "1 D").strip()
        bar_setting = self._BAR_SIZE_MAP.get(per.lower(), "1 day")

        one_day = 24 * 60 * 60
        if bar_setting == "1 day":
            chunk_seconds = 365 * one_day
            duration_str = "1 Y"
        elif bar_setting in ("1 hour", "5 mins"):
            chunk_seconds = 7 * one_day
            duration_str = "1 W"
        elif bar_setting == "1 min":
            chunk_seconds = one_day
            duration_str = "1 D"
        else:
            chunk_seconds = 7 * one_day
            duration_str = "1 W"

        end_ts_eff: float
        if end_ts is None:
            end_ts_eff = datetime.now(tz=timezone.utc).timestamp()
        else:
            end_ts_eff = float(end_ts)

        start_ts_eff: Optional[float] = float(start_ts) if start_ts is not None else None

        try:
            stock = self._stock(sym)
            await self.ib.qualifyContractsAsync(stock)
        except Exception as e:
            if self._is_connection_dropped(e):
                self._mark_connection_dropped(f"qualifyContracts {sym}", e)
                raise IBConnectionDroppedError(str(e)) from e
            logger.warning("get_historical_bars_range: qualifyContracts failed for %s: %s", sym, e, exc_info=True)
            return []

        use_rth = bar_setting != "1 day"
        all_out: List[Dict[str, Any]] = []

        cur_end = end_ts_eff
        loops = 0
        while True:
            loops += 1
            if loops > 2000:
                logger.warning("get_historical_bars_range: aborting after %s loops for %s %s", loops, sym, per)
                break

            if start_ts_eff is not None:
                if cur_end <= start_ts_eff:
                    break
                seg_start = max(start_ts_eff, cur_end - chunk_seconds)
            else:
                seg_start = cur_end - chunk_seconds

            end_dt = datetime.fromtimestamp(cur_end, tz=timezone.utc)
            end_str = end_dt.strftime("%Y%m%d-%H:%M:%S")

            try:
                bars = await self.ib.reqHistoricalDataAsync(
                    stock,
                    endDateTime=end_str,
                    durationStr=duration_str,
                    barSizeSetting=bar_setting,
                    whatToShow="TRADES",
                    useRTH=use_rth,
                    formatDate=2,
                )
            except Exception as e:
                if self._is_connection_dropped(e):
                    self._mark_connection_dropped(f"reqHistoricalDataAsync {sym} {per}", e)
                    raise IBConnectionDroppedError(str(e)) from e
                logger.warning(
                    "get_historical_bars_range: reqHistoricalDataAsync failed for %s %s: %s",
                    sym,
                    per,
                    e,
                    exc_info=True,
                )
                break

            chunk_out = self._convert_ib_bars(bars)
            if not chunk_out:
                break

            all_out.extend(chunk_out)

            if loops >= 1:
                if interval_sec is not None:
                    if interval_sec > 0:
                        await asyncio.sleep(interval_sec)
                else:
                    _time.sleep(0.35)

            cur_end = seg_start
            if start_ts_eff is None:
                break

        logger.info(
            "get_historical_bars_range: %s %s from %s to %s → %s bars (loops=%s)",
            sym,
            per,
            start_ts_eff,
            end_ts_eff,
            len(all_out),
            loops,
        )
        return all_out
