"""Dispatch IB Operator ops to OperatorIbClient / secondary AccountIbClient."""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

from bifrost_socket.ib.connector.ib_client import AccountIbClient, OperatorIbClient

logger = logging.getLogger(__name__)


def _jsonish_connected(v: Any) -> bool:
    if v is True:
        return True
    if v is False or v is None:
        return False
    if isinstance(v, (int, float)):
        return v != 0
    if isinstance(v, str):
        return v.strip().lower() in ("1", "true", "yes")
    return False


class IbOperatorExecutor:
    """Holds long-lived IB clients; async methods run on each client's internal loop."""

    def __init__(
        self,
        *,
        primary: OperatorIbClient,
        account_secondary: Optional[AccountIbClient],
    ) -> None:
        self._primary = primary
        self._account_secondary = account_secondary
        self._cmd_count = 0
        self._last_cmd_ts = 0.0
        self._host_ib_probe_at = 0.0
        self._host_ib_probe_ok = False
        self._secondary_ib_probe_at = 0.0
        self._secondary_ib_probe_ok = False
        self._ib_probe_interval_sec = 5.0

    def note_cmd_processed(self) -> None:
        self._cmd_count += 1
        self._last_cmd_ts = time.time()

    def _account_for_slot(self, payload: Dict[str, Any]) -> Any:
        slot = (payload.get("account_slot") or "primary").strip().lower()
        if slot == "secondary" and self._account_secondary is not None:
            return self._account_secondary
        return self._primary

    async def execute(self, op: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        if op == "ping":
            return {"ok": True, "data": self.health_dict()}
        if op == "disconnect_all":
            await self.disconnect_all()
            return {"ok": True, "data": self.health_dict()}
        if op == "reconnect_all":
            await self.connect_all()
            return {"ok": True, "data": self.health_dict()}
        if op == "fetch_executions":
            days = int(payload.get("days") or 7)
            acc = self._account_for_slot(payload)
            rows = await acc.fetch_executions(days=days)
            return {"ok": True, "data": {"executions": rows}}
        if op == "fetch_accounts_snapshot":
            acc = self._account_for_slot(payload)
            rows = await acc.fetch_accounts_snapshot()
            return {"ok": True, "data": {"accounts": rows}}
        if op == "place_stock_order":
            symbol = str(payload.get("symbol") or "").strip()
            side = str(payload.get("side") or "").strip()
            qty = int(payload.get("quantity") or 0)
            order_type = str(payload.get("order_type") or "market").strip().lower()
            limit_price = payload.get("limit_price")
            if not symbol or qty <= 0:
                return {"ok": False, "error": "missing_symbol_or_quantity"}
            acc = self._primary
            await acc._ensure_connected_impl()
            co = acc.connector
            if co is None:
                return {"ok": False, "error": "no_connector"}
            trade = await co.place_order(
                symbol, side, qty, order_type=order_type, limit_price=limit_price,
            )
            if trade is None:
                return {"ok": False, "error": "place_order_returned_none"}
            order = getattr(trade, "order", None)
            oid = getattr(order, "orderId", None) if order is not None else None
            return {"ok": True, "data": {"order_id": oid}}
        if op == "fetch_bars":
            symbol = str(payload.get("symbol") or "").strip()
            period = str(payload.get("period") or "1 day").strip()
            duration = str(payload.get("duration") or "1 D").strip()
            if not symbol:
                return {"ok": False, "error": "missing_symbol"}
            bars = await self._primary.fetch_bars(symbol, period, duration)
            return {"ok": True, "data": {"bars": bars}}
        if op == "fetch_bars_range":
            symbol = str(payload.get("symbol") or "").strip()
            period = str(payload.get("period") or "1 D").strip()
            if not symbol:
                return {"ok": False, "error": "missing_symbol"}
            start_ts = payload.get("start_ts")
            end_ts = payload.get("end_ts")
            interval = payload.get("interval_sec")
            st = float(start_ts) if start_ts is not None else None
            et = float(end_ts) if end_ts is not None else None
            iv = float(interval) if interval is not None and float(interval) > 0 else None
            bars = await self._primary.fetch_bars_range(symbol, period, start_ts=st, end_ts=et, interval_sec=iv)
            return {"ok": True, "data": {"bars": bars}}
        if op == "fetch_option_expirations":
            symbol = str(payload.get("symbol") or "").strip()
            if not symbol:
                return {"ok": False, "error": "missing_symbol"}
            out = await self._primary.fetch_option_expirations(symbol)
            return {"ok": True, "data": out}
        if op == "fetch_option_snapshot":
            symbol = str(payload.get("symbol") or "").strip()
            expiration = str(payload.get("expiration") or "").strip()
            strikes = payload.get("strikes") or []
            if not isinstance(strikes, list):
                strikes = []
            strikes_f: List[float] = []
            for s in strikes:
                try:
                    strikes_f.append(float(s))
                except (TypeError, ValueError):
                    pass
            max_contracts = int(payload.get("max_contracts") or 20)
            pacing_sec = float(payload.get("pacing_sec") or 0.35)
            if not symbol or not expiration:
                return {"ok": False, "error": "missing_symbol_or_expiration"}
            rows, underlying_price = await self._primary.fetch_option_snapshot(
                symbol, expiration, strikes_f, max_contracts=max_contracts, pacing_sec=pacing_sec,
            )
            return {"ok": True, "data": {"rows": rows, "underlying_price": underlying_price}}
        return {"ok": False, "error": f"unhandled_op:{op}"}

    def health_dict(self) -> Dict[str, Any]:
        def _conn(c: Any) -> bool:
            snap = getattr(c, "connected_snapshot", None)
            if callable(snap):
                return _jsonish_connected(snap())
            return _jsonish_connected(getattr(c, "connected", False))

        out: Dict[str, Any] = {
            "host": {
                "connected": _conn(self._primary),
                "client_id": int(getattr(self._primary, "client_id", 0)),
                "last_error": getattr(self._primary, "last_error", None),
                "reconnects": int(getattr(self._primary, "reconnects", 0)),
                "ib_probe_at": float(self._host_ib_probe_at),
                "ib_probe_ok": bool(self._host_ib_probe_ok),
                "ib_probe_interval_sec": float(self._ib_probe_interval_sec),
            },
            "service_alive": True,
            "cmd_count": self._cmd_count,
            "last_cmd_ts": self._last_cmd_ts,
        }
        if self._account_secondary is not None:
            out["secondary"] = {
                "connected": _conn(self._account_secondary),
                "client_id": int(getattr(self._account_secondary, "client_id", 0)),
                "last_error": getattr(self._account_secondary, "last_error", None),
                "reconnects": int(getattr(self._account_secondary, "reconnects", 0)),
                "ib_probe_at": float(self._secondary_ib_probe_at),
                "ib_probe_ok": bool(self._secondary_ib_probe_ok),
                "ib_probe_interval_sec": float(self._ib_probe_interval_sec),
            }
        else:
            out["secondary"] = None
        return out

    async def record_ib_probe(self, interval_sec: float) -> None:
        self._ib_probe_interval_sec = float(interval_sec)
        now = time.time()
        self._host_ib_probe_at = now
        self._host_ib_probe_ok = bool(self._primary.connected_snapshot())
        if self._account_secondary is not None:
            self._secondary_ib_probe_at = now
            self._secondary_ib_probe_ok = bool(self._account_secondary.connected_snapshot())
        else:
            self._secondary_ib_probe_at = 0.0
            self._secondary_ib_probe_ok = False

    async def connect_all(self, *, max_connect_attempts: Optional[int] = None) -> None:
        await self.connect_primary_only(max_connect_attempts=max_connect_attempts)
        await self.connect_secondary_only(max_connect_attempts=max_connect_attempts)

    async def connect_primary_only(self, *, max_connect_attempts: Optional[int] = None) -> None:
        await self._primary.ensure_connected(max_connect_attempts=max_connect_attempts)

    async def connect_secondary_only(self, *, max_connect_attempts: Optional[int] = None) -> None:
        if self._account_secondary is not None:
            await self._account_secondary.ensure_connected(max_connect_attempts=max_connect_attempts)

    async def disconnect_all(self) -> None:
        try:
            await self._primary.disconnect()
        except Exception as e:
            logger.debug("disconnect primary: %s", e)
        if self._account_secondary is not None:
            try:
                await self._account_secondary.disconnect()
            except Exception as e:
                logger.debug("disconnect secondary: %s", e)
