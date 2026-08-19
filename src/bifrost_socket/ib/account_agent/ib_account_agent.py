"""IB Account Agent — account-domain IB events → Redis.

Subscribes to positions, orders, and fills; writes periodic account snapshots to
Redis ``ib:account:snapshot:v1`` (JSON), stream ``ib:account:stream:v1``, and
health hash ``bifrost:health:ws_ib_account_agent`` (including env/config_file).

Supports optional Secondary TWS slot (dual-slot concurrent reconnect, which is
intentional and correct — unlike the Ingestor's B2 bug, the Agent's two slots are
independent IB connections that should reconnect in parallel).

Bug fixes vs legacy run_ib_account_agent.py:
  B1 — config path uses resolve_config_path() (env precedence: prod before dev)
  B4 — health hash includes env/config_file via HealthHashWriter
"""

from __future__ import annotations

import asyncio
import logging
import signal
import time
from typing import Any, Dict, List, Optional

from bifrost_core.core.message_center import IbConnectionStatusTracker
from bifrost_core.ws_client.retry import ReconnectPolicy

from bifrost_socket.config import (
    detect_env,
    get_effective_ib_config,
    make_redis_client,
)
from bifrost_socket.ib.account_agent.redis_writer import IbAccountAgentRedisWriter
from bifrost_socket.ib.connection_lifecycle import (
    REASON_SESSION_ENDED,
    HeartbeatReconnectTarget,
    ServiceHeartbeatClock,
    heartbeat_reconnect_slots_parallel,
    publish_account_agent_slots,
    publish_service_stopped_disconnects,
    resolve_ib_broker_lifecycle,
)
from bifrost_socket.ib.connector.ib_client import AccountIbClient

logger = logging.getLogger(__name__)

SNAPSHOT_POLL_SEC = 2.0
HOST_FAIL_ITERATIONS_BEFORE_SESSION_RESET = 15
_POST_CONNECT_SNAPSHOT_ATTEMPTS = 20
_POST_CONNECT_SNAPSHOT_DELAY_SEC = 0.25


def _merged_open_orders(primary: AccountIbClient, secondary: Optional[AccountIbClient]) -> List[Dict[str, Any]]:
    orders: List[Dict[str, Any]] = []
    seen: set = set()
    hc = primary.connector
    if hc and primary._connected_state:
        try:
            orders = list(hc.get_open_orders_snapshot() or [])
            seen = {(o.get("order_id"), o.get("account_id")) for o in orders}
        except Exception as e:
            logger.warning("host open orders snapshot: %s", e)
    if secondary is not None:
        sc = secondary.connector
        if sc and secondary._connected_state:
            try:
                for o in sc.get_open_orders_snapshot() or []:
                    key = (o.get("order_id"), o.get("account_id"))
                    if key not in seen:
                        seen.add(key)
                        orders.append(o)
            except Exception as e:
                logger.warning("secondary open orders snapshot: %s", e)
    return orders


async def _accounts_snapshot(
    primary: AccountIbClient,
    secondary: Optional[AccountIbClient],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    hc = primary.connector
    if hc and primary._connected_state:
        account_ids = hc.get_managed_accounts()
        if account_ids:
            all_p = await hc.get_positions(account=None)
            for aid in account_ids:
                values = await hc.get_account_summary(account=aid)
                summary: Dict[str, Any] = {}
                for v in values:
                    if getattr(v, "tag", None) and getattr(v, "value", None) is not None:
                        summary[v.tag] = v.value
                if aid:
                    summary["account"] = aid
                acct_p = [p for p in all_p if getattr(p, "account", None) == aid]
                out.append({
                    "account_id": aid,
                    "summary": summary,
                    "positions": [hc.position_to_dict(p) for p in acct_p],
                })

    if secondary is not None:
        sc = secondary.connector
        if sc and secondary._connected_state:
            ids2 = sc.get_managed_accounts()
            if ids2:
                all_p2 = await sc.get_positions(account=None)
                for aid in ids2:
                    if any(a.get("account_id") == aid for a in out):
                        continue
                    values = await sc.get_account_summary(account=aid)
                    summary = {}
                    for v in values:
                        if getattr(v, "tag", None) and getattr(v, "value", None) is not None:
                            summary[v.tag] = v.value
                    if aid:
                        summary["account"] = aid
                    acct_p = [p for p in all_p2 if getattr(p, "account", None) == aid]
                    out.append({
                        "account_id": aid,
                        "summary": summary,
                        "positions": [sc.position_to_dict(p) for p in acct_p],
                    })
    return out


class IbAccountAgent:
    """IB Account Agent process."""

    def __init__(self, cfg: Dict[str, Any]) -> None:
        self._cfg = cfg
        self._rds = make_redis_client(cfg)
        env = detect_env(cfg.get("_config_file", ""))
        self._writer = IbAccountAgentRedisWriter(
            self._rds,
            env=env,
            config_file=cfg.get("_config_file", ""),
        )
        self._tracker = IbConnectionStatusTracker(self._rds, service="ib_account_agent")
        self._stop = asyncio.Event()
        self._session_disconnected = asyncio.Event()
        self._reconnects = 0
        self._msg_count = 0
        self._last_msg_ts = 0.0
        self._host_cid = 0
        self._sec_cid: Optional[int] = None
        self._lifecycle = resolve_ib_broker_lifecycle(cfg, "ib_account_agent")
        self._hb_clock = ServiceHeartbeatClock(self._lifecycle.service_heartbeat_interval_sec)
        self._probe_interval_sec = self._lifecycle.probe_interval_sec
        self._host_probe_at = 0.0
        self._host_probe_ok = False
        self._sec_probe_at = 0.0
        self._sec_probe_ok = False
        self._fill_rows: List[Dict[str, Any]] = []

    def _bump(self) -> None:
        self._msg_count += 1
        self._last_msg_ts = time.time()

    def _push_health(
        self,
        *,
        host_connected: bool,
        last_msg_ts: float,
        reconnects: int,
        msg_count: int,
        secondary_connected: Optional[bool] = None,
        secondary_client_id: Optional[int] = None,
        host_alive: bool = True,
        host_probe_at: float = 0.0,
        host_probe_ok: bool = False,
        host_probe_interval_sec: float = 0.0,
        secondary_probe_at: float = 0.0,
        secondary_probe_ok: bool = False,
        secondary_probe_interval_sec: float = 0.0,
        service_heartbeat_reconnect_in_progress: str = "",
        reason: Optional[str] = None,
        publish_messages: bool = True,
    ) -> None:
        now = time.time()
        sh = self._hb_clock.redis_fields(
            now,
            reconnect_in_progress=service_heartbeat_reconnect_in_progress,
        )
        self._writer.write_health(
            host_client_id=self._host_cid,
            host_connected=host_connected,
            last_msg_ts=last_msg_ts,
            reconnects=reconnects,
            msg_count=msg_count,
            secondary_connected=secondary_connected,
            secondary_client_id=secondary_client_id,
            host_alive=host_alive,
            host_probe_at=host_probe_at,
            host_probe_ok=host_probe_ok,
            host_probe_interval_sec=host_probe_interval_sec,
            secondary_probe_at=secondary_probe_at,
            secondary_probe_ok=secondary_probe_ok,
            secondary_probe_interval_sec=secondary_probe_interval_sec,
            service_heartbeat_interval_sec=float(sh["service_heartbeat_interval_sec"]),
            last_service_heartbeat_at=float(sh["last_service_heartbeat_at"]),
            next_service_heartbeat_in_s=float(sh["next_service_heartbeat_in_s"]),
            service_heartbeat_reconnect_in_progress=str(
                sh.get("service_heartbeat_reconnect_in_progress") or ""
            ),
        )
        if publish_messages:
            publish_account_agent_slots(
                self._tracker,
                host_connected=host_connected,
                host_client_id=self._host_cid or None,
                occurred_at=last_msg_ts,
                secondary_connected=secondary_connected,
                secondary_client_id=secondary_client_id,
                reason=reason,
            )

    def _publish_service_stopped_messages(self, *, has_secondary: bool) -> None:
        slots: list[tuple[str, int]] = []
        if self._host_cid > 0:
            slots.append(("host", int(self._host_cid)))
        if has_secondary and self._sec_cid and int(self._sec_cid) > 0:
            slots.append(("secondary", int(self._sec_cid)))
        publish_service_stopped_disconnects(
            self._rds,
            mc_service="ib_account_agent",
            slot_client_ids=slots,
        )

    async def run(self) -> None:
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._stop.set)
            except NotImplementedError:
                pass

        ib_cfg = get_effective_ib_config(self._cfg)
        self._lifecycle = resolve_ib_broker_lifecycle(self._cfg, "ib_account_agent")
        self._hb_clock = ServiceHeartbeatClock(
            self._lifecycle.service_heartbeat_interval_sec,
            last_at=time.time(),
        )
        self._probe_interval_sec = self._lifecycle.probe_interval_sec
        self._host_cid = int(ib_cfg["client_id_account_agent"])
        host = ib_cfg["host"]
        port = ib_cfg["port"]
        policy = ReconnectPolicy.from_config(self._cfg)

        logger.info(
            "IB Account Agent starting host=%s port=%s client_id=%s",
            host,
            port,
            self._host_cid,
        )

        # B5 design (same as Ingestor): create clients once per process lifetime.
        primary = AccountIbClient(host, port, self._host_cid, name="IbAccountAgentHost")

        secondary: Optional[AccountIbClient] = None
        ib2_host = ib_cfg.get("ib2_host") or ""
        if ib2_host:
            self._sec_cid = int(ib_cfg["ib2_client_id_account_agent"])
            secondary = AccountIbClient(
                ib2_host,
                ib_cfg["ib2_port"],
                self._sec_cid,
                name="IbAccountAgentSecondary",
            )

        self._push_health(
            host_connected=False,
            last_msg_ts=time.time(),
            reconnects=self._reconnects,
            msg_count=self._msg_count,
            secondary_connected=False if secondary is not None else None,
            secondary_client_id=self._sec_cid,
        )

        attempt = 0
        while not self._stop.is_set():
            try:
                await self._run_session(primary, secondary)
                attempt = 0
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Account agent session error: %s", e, exc_info=True)
                attempt += 1

            if self._stop.is_set():
                break
            self._reconnects += 1
            delay = policy.delay_for_attempt(attempt)
            self._push_health(
                host_connected=False,
                last_msg_ts=self._last_msg_ts or time.time(),
                reconnects=self._reconnects,
                msg_count=self._msg_count,
                secondary_connected=False if secondary is not None else None,
                secondary_client_id=self._sec_cid,
                reason=REASON_SESSION_ENDED,
            )
            logger.info(
                "Next session reconnect in %.1fs (attempt %d)…", delay, self._reconnects
            )
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass

        self._publish_service_stopped_messages(has_secondary=secondary is not None)
        self._push_health(
            host_connected=False,
            last_msg_ts=self._last_msg_ts or time.time(),
            reconnects=self._reconnects,
            msg_count=self._msg_count,
            host_alive=False,
            secondary_connected=False if secondary is not None else None,
            secondary_client_id=self._sec_cid,
            publish_messages=False,
        )
        logger.info("IB Account Agent stopped")

    async def _run_session(
        self,
        primary: AccountIbClient,
        secondary: Optional[AccountIbClient],
    ) -> None:
        async def _host_connect_and_settle() -> None:
            await primary.ensure_connected()
            for _ in range(_POST_CONNECT_SNAPSHOT_ATTEMPTS):
                if primary.connected_snapshot():
                    break
                await asyncio.sleep(_POST_CONNECT_SNAPSHOT_DELAY_SEC)

        async def _secondary_connect_and_settle() -> None:
            if secondary is None:
                return
            try:
                await secondary.ensure_connected()
                for _ in range(_POST_CONNECT_SNAPSHOT_ATTEMPTS):
                    if secondary.connected_snapshot():
                        break
                    await asyncio.sleep(_POST_CONNECT_SNAPSHOT_DELAY_SEC)
            except Exception as e:
                logger.warning("Secondary connect failed: %s", e)

        self._reconnects = 0
        self._session_disconnected.clear()
        self._fill_rows.clear()

        # Dual-slot connect in parallel (intentional — independent TWS connections).
        if secondary is None:
            await _host_connect_and_settle()
        else:
            results = await asyncio.gather(
                _host_connect_and_settle(),
                _secondary_connect_and_settle(),
                return_exceptions=True,
            )
            if isinstance(results[0], BaseException):
                raise results[0]

        hc = primary.connector
        sc = secondary.connector if secondary else None
        host_ok = bool(primary.connected_snapshot())
        sec_ok = bool(secondary.connected_snapshot()) if secondary is not None else False

        now0 = time.time()
        self._host_probe_at = now0
        self._host_probe_ok = host_ok
        if secondary is not None:
            self._sec_probe_at = now0
            self._sec_probe_ok = sec_ok

        self._push_health(
            host_connected=host_ok,
            last_msg_ts=now0,
            reconnects=self._reconnects,
            msg_count=self._msg_count,
            secondary_connected=sec_ok if secondary is not None else None,
            secondary_client_id=self._sec_cid,
            host_probe_at=now0,
            host_probe_ok=host_ok,
            secondary_probe_at=now0 if secondary is not None else 0.0,
            secondary_probe_ok=sec_ok,
        )

        probe_task = asyncio.ensure_future(self._probe_loop(primary, secondary))

        if hc and host_ok:
            hc.subscribe_positions(lambda: self._bump())
            hc.subscribe_order_status(lambda _: self._bump())
            hc.subscribe_open_order(lambda _: self._bump())
            try:
                hc.subscribe_fills(lambda _t, _f: self._bump())
            except Exception as e:
                logger.warning("subscribe_fills host: %s", e)
            try:
                await hc.get_open_orders_async(include_all_from_tws=True)
            except Exception as e:
                logger.warning("reqAllOpenOrders host: %s", e)

        if sc and sec_ok:
            try:
                sc.subscribe_positions(lambda: self._bump())
                sc.subscribe_order_status(lambda _: self._bump())
                sc.subscribe_open_order(lambda _: self._bump())

                def on_sec_fill(_trade: Any, fill: Any) -> None:
                    self._bump()
                    row = sc.fill_to_execution_row(fill, source="tws_event")
                    if row:
                        self._fill_rows.append(row)

                sc.subscribe_fills(on_sec_fill)
                await sc.get_open_orders_async(include_all_from_tws=True)
            except Exception as e:
                logger.warning("Secondary subscriptions: %s", e)

        host_fail_streak = 0
        try:
            while not self._stop.is_set():
                if self._session_disconnected.is_set():
                    raise ConnectionError("IB Account Agent disconnect — resetting session")
                try:
                    host_ok = bool(primary.connected_snapshot())
                    sec_ok = bool(secondary.connected_snapshot()) if secondary is not None else False

                    if not host_ok:
                        host_fail_streak += 1
                        if host_fail_streak >= HOST_FAIL_ITERATIONS_BEFORE_SESSION_RESET:
                            raise ConnectionError("IB Account Agent host API disconnected; resetting")
                    else:
                        host_fail_streak = 0

                    oo = _merged_open_orders(primary, secondary)
                    acct = await _accounts_snapshot(primary, secondary)
                    fills = list(self._fill_rows)
                    self._fill_rows.clear()
                    self._writer.write_snapshot({
                        "open_orders": oo,
                        "accounts_snapshot": acct,
                        "last_execution_rows": fills,
                        "host_connected": host_ok,
                        "secondary_connected": sec_ok,
                    })
                    self._push_health(
                        host_connected=host_ok,
                        last_msg_ts=self._last_msg_ts or time.time(),
                        reconnects=self._reconnects,
                        msg_count=self._msg_count,
                        secondary_connected=sec_ok if secondary is not None else None,
                        secondary_client_id=self._sec_cid,
                        host_probe_at=self._host_probe_at,
                        host_probe_ok=self._host_probe_ok,
                        secondary_probe_at=self._sec_probe_at,
                        secondary_probe_ok=self._sec_probe_ok,
                    )
                except ConnectionError:
                    raise
                except Exception as e:
                    logger.warning("snapshot iteration: %s", e)
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=SNAPSHOT_POLL_SEC)
                except asyncio.TimeoutError:
                    pass
        finally:
            probe_task.cancel()
            try:
                await probe_task
            except asyncio.CancelledError:
                pass
            try:
                await primary.disconnect()
            except Exception:
                pass
            if secondary is not None:
                try:
                    await secondary.disconnect()
                except Exception:
                    pass

    async def _probe_loop(
        self,
        primary: AccountIbClient,
        secondary: Optional[AccountIbClient],
    ) -> None:
        """Periodic IB liveness probe.

        Reads _connected_state directly (not connected_snapshot()) to avoid blocking
        during long account-snapshot RPCs that occupy the IB client's event loop.

        Also handles service heartbeat reconnects for both slots in parallel.
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

            host_ok = bool(getattr(primary, "_connected_state", False))
            sec_ok = (
                bool(getattr(secondary, "_connected_state", False))
                if secondary is not None
                else False
            )
            if not host_ok:
                self._session_disconnected.set()

            if self._hb_clock.tick(now):
                hb_targets: List[HeartbeatReconnectTarget] = []
                hint_parts: List[str] = []
                if not host_ok:
                    hint_parts.append(
                        ServiceHeartbeatClock.reconnect_hint_part("Host", self._host_cid)
                    )

                    async def _reconnect_host() -> None:
                        await primary.ensure_connected(max_connect_attempts=1)

                    hb_targets.append(
                        HeartbeatReconnectTarget(
                            slot_label="Host",
                            client_id=self._host_cid,
                            reconnect=_reconnect_host,
                        )
                    )
                if secondary is not None and not sec_ok:
                    sec_cid = int(self._sec_cid or 0)
                    hint_parts.append(
                        ServiceHeartbeatClock.reconnect_hint_part("Secondary", sec_cid)
                    )

                    async def _reconnect_secondary() -> None:
                        await secondary.ensure_connected(max_connect_attempts=1)

                    hb_targets.append(
                        HeartbeatReconnectTarget(
                            slot_label="Secondary",
                            client_id=sec_cid,
                            reconnect=_reconnect_secondary,
                        )
                    )
                if hb_targets:
                    reconnect_hint = ServiceHeartbeatClock.reconnect_hint_join(*hint_parts)
                    self._push_health(
                        host_connected=host_ok,
                        last_msg_ts=self._last_msg_ts or now,
                        reconnects=self._reconnects,
                        msg_count=self._msg_count,
                        secondary_connected=sec_ok if secondary is not None else None,
                        secondary_client_id=self._sec_cid,
                        host_probe_at=self._host_probe_at,
                        host_probe_ok=self._host_probe_ok,
                        secondary_probe_at=self._sec_probe_at,
                        secondary_probe_ok=self._sec_probe_ok,
                        host_probe_interval_sec=self._probe_interval_sec,
                        secondary_probe_interval_sec=self._probe_interval_sec,
                        service_heartbeat_reconnect_in_progress=reconnect_hint,
                    )
                    await heartbeat_reconnect_slots_parallel(
                        hb_targets,
                        connect_timeout_sec=self._lifecycle.connect_timeout_sec,
                        log_prefix="IB account agent",
                    )
                    host_ok = bool(getattr(primary, "_connected_state", False))
                    sec_ok = (
                        bool(getattr(secondary, "_connected_state", False))
                        if secondary is not None
                        else False
                    )
                    reconnect_hint = ""

            self._host_probe_at = now
            self._host_probe_ok = host_ok
            if secondary is not None:
                self._sec_probe_at = now
                self._sec_probe_ok = sec_ok
            try:
                self._push_health(
                    host_connected=host_ok,
                    last_msg_ts=self._last_msg_ts or now,
                    reconnects=self._reconnects,
                    msg_count=self._msg_count,
                    secondary_connected=sec_ok if secondary is not None else None,
                    secondary_client_id=self._sec_cid,
                    host_probe_at=now,
                    host_probe_ok=host_ok,
                    secondary_probe_at=now if secondary is not None else 0.0,
                    secondary_probe_ok=sec_ok,
                    host_probe_interval_sec=self._probe_interval_sec,
                    secondary_probe_interval_sec=self._probe_interval_sec,
                    service_heartbeat_reconnect_in_progress=reconnect_hint,
                )
            except Exception as e:
                logger.debug("probe health write: %s", e)
