"""IB Operator main loop: Redis Stream consumer + IB executor."""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from typing import Any, Dict, Optional

import redis

from bifrost_core.core.message_center import IbConnectionStatusTracker

from bifrost_socket.config import detect_env, get_effective_ib_config, make_redis_client
from bifrost_socket.ib.connection_lifecycle import (
    HeartbeatReconnectTarget,
    ServiceHeartbeatClock,
    heartbeat_reconnect_slots_parallel,
    publish_operator_slots,
    publish_service_stopped_disconnects,
    resolve_ib_broker_lifecycle,
    run_heartbeat_reconnect_sync,
)
from bifrost_socket.ib.connector.ib_client import AccountIbClient, OperatorIbClient
from bifrost_socket.ib.operator.config import effective_ib_operator_settings
from bifrost_socket.ib.operator.executor import IbOperatorExecutor
from bifrost_socket.ib.operator.health_writer import IbOperatorHealthWriter
from bifrost_socket.ib.operator.protocol import (
    CommandMessage,
    dumps_result,
    parse_stream_fields,
    result_key,
)
from bifrost_socket.ib.operator.redis_io import (
    OperatorRedisRunner,
    ack_message,
    consumer_name,
    ensure_stream_and_group,
    parse_xreadgroup_reply,
    write_result,
    xreadgroup_recover_nogroup,
)
logger = logging.getLogger(__name__)

# Serialize IB asyncio.run() (RPC connect/reconnect/execute) on the main thread only.
# Probe health refresh runs on a separate daemon thread and never calls asyncio.run.
_OPERATOR_MAIN_LOCK = threading.Lock()


class _ReconnectHint:
    """Thread-safe reconnect-in-progress label for probe health writes."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._value = ""

    def set(self, value: str) -> None:
        with self._lock:
            self._value = value

    def get(self) -> str:
        with self._lock:
            return self._value


class _OperatorProbeThread(threading.Thread):
    """Periodic IB liveness + Redis health (Account Agent probe-loop parity).

    Reads ``_connected_state`` only — no asyncio.run, no ``_OPERATOR_MAIN_LOCK``.
    """

    def __init__(
        self,
        *,
        stop: threading.Event,
        executor: IbOperatorExecutor,
        writer: IbOperatorHealthWriter,
        probe_interval_sec: float,
        tracker: Optional[IbConnectionStatusTracker],
        hb_clock: ServiceHeartbeatClock,
        reconnect_hint: _ReconnectHint,
    ) -> None:
        super().__init__(name="ib-operator-probe", daemon=True)
        self._stop_event = stop
        self._executor = executor
        self._writer = writer
        self._probe_iv = float(probe_interval_sec)
        self._tracker = tracker
        self._hb_clock = hb_clock
        self._reconnect_hint = reconnect_hint

    def run(self) -> None:
        while not self._stop_event.wait(timeout=max(1.0, self._probe_iv)):
            if self._stop_event.is_set():
                break
            try:
                self._tick()
            except Exception as e:
                logger.debug("probe thread tick: %s", e)

    def write_health_now(self) -> None:
        """Immediate health write (e.g. after reconnect); safe from main thread."""
        self._tick()

    def _tick(self) -> None:
        self._executor.record_ib_probe(self._probe_iv)
        h = self._executor.health_dict()
        now = time.time()
        hint = self._reconnect_hint.get()
        h["updated_at"] = now
        h.update(self._hb_clock.redis_fields(now, reconnect_in_progress=hint))
        if hint:
            h["service_heartbeat_reconnect_in_progress"] = hint
        self._writer.write(h)
        if self._tracker is not None:
            publish_operator_slots(self._tracker, h)


def _build_executor(config: Dict[str, Any]) -> IbOperatorExecutor:
    ib_cfg = get_effective_ib_config(config)
    primary = OperatorIbClient(
        host=ib_cfg["host"],
        port=int(ib_cfg["port"]),
        client_id=ib_cfg["client_id_operator"],
        name="IbOperator",
    )
    acc2: Optional[AccountIbClient] = None
    ib2_host = ib_cfg.get("ib2_host") or ""
    if ib2_host:
        acc2 = AccountIbClient(
            host=ib2_host,
            port=ib_cfg["ib2_port"],
            client_id=ib_cfg["ib2_client_id_operator"],
            name="IbOperatorAccount2",
        )
    return IbOperatorExecutor(primary=primary, account_secondary=acc2)


def _write_health(
    writer: IbOperatorHealthWriter,
    executor: IbOperatorExecutor,
    probe_interval_sec: float,
    tracker: Optional[IbConnectionStatusTracker] = None,
    *,
    hb_clock: Optional[ServiceHeartbeatClock] = None,
    service_heartbeat_reconnect_in_progress: Optional[str] = None,
) -> None:
    try:
        executor.record_ib_probe(probe_interval_sec)
    except Exception as e:
        logger.debug("record_ib_probe: %s", e)
    h = executor.health_dict()
    now = time.time()
    h["updated_at"] = now
    if service_heartbeat_reconnect_in_progress is not None:
        h["service_heartbeat_reconnect_in_progress"] = service_heartbeat_reconnect_in_progress
    if hb_clock is not None:
        h.update(
            hb_clock.redis_fields(
                now,
                reconnect_in_progress=service_heartbeat_reconnect_in_progress or "",
            )
        )
    writer.write(h)
    if tracker is not None:
        publish_operator_slots(tracker, h)


def _operator_heartbeat_reconnect_slot(
    executor: IbOperatorExecutor,
    *,
    slot: str,
    client_id: int,
    connect_timeout_sec: float,
    reconnect_host: bool,
) -> None:
    if reconnect_host:
        async def _reconnect() -> None:
            await executor.connect_primary_only(max_connect_attempts=1)
    else:
        async def _reconnect() -> None:
            await executor.connect_secondary_only(max_connect_attempts=1)

    run_heartbeat_reconnect_sync(
        heartbeat_reconnect_slots_parallel(
            [
                HeartbeatReconnectTarget(
                    slot_label=slot,
                    client_id=client_id,
                    reconnect=_reconnect,
                )
            ],
            connect_timeout_sec=connect_timeout_sec,
            log_prefix="IB Operator",
        )
    )


async def _handle_message(executor: IbOperatorExecutor, msg: CommandMessage) -> Dict[str, Any]:
    try:
        return await executor.execute(msg.op, msg.payload)
    except Exception as e:
        logger.warning(
            "execute op=%s req_id=%s caller=%s: %s",
            msg.op, msg.req_id, msg.caller, e,
            exc_info=True,
        )
        return {"ok": False, "error": str(e)}


def run_ib_operator_loop(
    config: Dict[str, Any],
    *,
    stop_event: Optional[threading.Event] = None,
    redis_client: Optional[redis.Redis] = None,
) -> None:
    """Block until stop_event is set. Creates IB clients and consumes the Redis cmd stream."""
    settings = effective_ib_operator_settings(config)
    if not settings["enabled"]:
        logger.error("IB Operator disabled in config (ib_operator.enabled=false or no Redis).")
        return

    env = detect_env(config.get("_config_file", ""))
    config_file = str(config.get("_config_file") or "")

    r = redis_client or make_redis_client(config)
    tracker = IbConnectionStatusTracker(r, service="ib_operator")
    writer = IbOperatorHealthWriter(r, env=env, config_file=config_file)

    stream = settings["stream"]
    group = settings["consumer_group"]
    cons = consumer_name()
    block_ms = settings["block_ms"]
    result_ttl = settings["result_ttl_sec"]
    result_prefix = settings["result_prefix"]
    max_bytes = settings["max_result_bytes"]
    lifecycle = resolve_ib_broker_lifecycle(config, "ib_operator")
    probe_iv = lifecycle.probe_interval_sec
    hb_clock = ServiceHeartbeatClock(lifecycle.service_heartbeat_interval_sec, last_at=time.time())

    ensure_stream_and_group(r, stream, group)
    executor = _build_executor(config)
    runner = OperatorRedisRunner(r, stream, group, cons, block_ms)

    logger.info("IB Operator started stream=%s group=%s consumer=%s", stream, group, cons)

    with _OPERATOR_MAIN_LOCK:
        _write_health(writer, executor, probe_iv, tracker, hb_clock=hb_clock)
        try:
            asyncio.run(executor.connect_primary_only())
        except Exception as e:
            logger.warning(
                "IB Operator initial Host connect failed (will retry on heartbeat): %s", e
            )
        _write_health(writer, executor, probe_iv, tracker, hb_clock=hb_clock)
        # Secondary connects on the first service heartbeat tick (non-blocking).
        # Blocking here stalled Redis probe refresh and made Host look stale while
        # TWS still showed clientId=20 connected (Ingestor/Account Agent do not block).

    stop = stop_event or threading.Event()
    reconnect_hint = _ReconnectHint()
    probe_thread = _OperatorProbeThread(
        stop=stop,
        executor=executor,
        writer=writer,
        probe_interval_sec=probe_iv,
        tracker=tracker,
        hb_clock=hb_clock,
        reconnect_hint=reconnect_hint,
    )
    probe_thread.start()

    try:
        while not stop.is_set():
            now = time.time()
            reconnect_job: Optional[tuple[str, int, bool]] = None
            if hb_clock.tick(now):
                host_ok, sec_ok = executor.slots_connected_snapshot()
                if not host_ok:
                    hid = int((executor.health_dict().get("host") or {}).get("client_id") or 0)
                    reconnect_hint.set(ServiceHeartbeatClock.reconnect_hint_part("Host", hid))
                    reconnect_job = ("Host", hid, True)
                elif sec_ok is not None and not sec_ok:
                    sid = int((executor.health_dict().get("secondary") or {}).get("client_id") or 0)
                    reconnect_hint.set(ServiceHeartbeatClock.reconnect_hint_part("Secondary", sid))
                    reconnect_job = ("Secondary", sid, False)
                else:
                    reconnect_hint.set("")

            if reconnect_job is not None:
                slot_label, client_id, reconnect_host = reconnect_job
                _operator_heartbeat_reconnect_slot(
                    executor,
                    slot=slot_label,
                    client_id=client_id,
                    connect_timeout_sec=lifecycle.connect_timeout_sec,
                    reconnect_host=reconnect_host,
                )
                reconnect_hint.set("")
                probe_thread.write_health_now()

            # Drain pending (PEL) first, then read new.
            entries = []
            try:
                reply = xreadgroup_recover_nogroup(
                    r, group, cons, {stream: "0"},
                    count=1, block=0,
                    stream_name=stream, group_name=group,
                )
                entries = parse_xreadgroup_reply(reply)
            except Exception as e:
                logger.warning("xreadgroup pending (stream=%s): %s", stream, e)

            if not entries and not stop.is_set():
                try:
                    entries = runner.read_new()
                except Exception as e:
                    # Transient Redis timeouts must not tear down IB sessions (finally → disconnect_all).
                    logger.warning("xreadgroup read_new (stream=%s): %s", stream, e)
                    continue

            if not entries:
                continue

            for entry_id, fields in entries:
                if stop.is_set():
                    break
                msg, perr = parse_stream_fields(fields, stream_id=entry_id)
                rk = result_key(result_prefix, msg.req_id if msg else "invalid")

                if perr or msg is None:
                    logger.warning("Bad operator message id=%s: %s", entry_id, perr)
                    err_body, _ = dumps_result({"ok": False, "error": perr or "parse"}, max_bytes=max_bytes)
                    if msg and msg.req_id:
                        write_result(r, rk, err_body or "{}", ttl_sec=result_ttl)
                    ack_message(r, stream, group, entry_id)
                    continue

                if msg.is_expired():
                    err_body, enc_err = dumps_result({"ok": False, "error": "deadline_expired"}, max_bytes=max_bytes)
                    if enc_err:
                        err_body = '{"ok":false,"error":"deadline_expired"}'
                    write_result(r, rk, err_body or "{}", ttl_sec=result_ttl)
                    ack_message(r, stream, group, entry_id)
                    continue

                with _OPERATOR_MAIN_LOCK:
                    outcome = asyncio.run(_handle_message(executor, msg))
                    executor.note_cmd_processed()
                body, enc_err = dumps_result(outcome, max_bytes=max_bytes)
                if enc_err:
                    body, _ = dumps_result({"ok": False, "error": enc_err}, max_bytes=max_bytes)
                write_result(r, rk, body or '{"ok":false,"error":"encode"}', ttl_sec=result_ttl)
                ack_message(r, stream, group, entry_id)
    finally:
        stop.set()
        probe_thread.join(timeout=max(2.0, probe_iv + 1.0))
        logger.info("IB Operator stopping: disconnecting IB clients")
        try:
            with _OPERATOR_MAIN_LOCK:
                asyncio.run(executor.disconnect_all())
        except Exception as e:
            logger.warning("disconnect on shutdown: %s", e)

    h_final = executor.health_dict()
    stop_slots: list[tuple[str, int]] = []
    for slot_name in ("host", "secondary"):
        sub = h_final.get(slot_name)
        if isinstance(sub, dict):
            cid = int(sub.get("client_id") or 0)
            if cid > 0:
                stop_slots.append((slot_name, cid))
            h_final[slot_name] = {**sub, "connected": False}
    h_final["service_alive"] = False
    h_final["updated_at"] = time.time()
    publish_service_stopped_disconnects(
        r,
        mc_service="ib_operator",
        slot_client_ids=stop_slots,
        occurred_at=float(h_final["updated_at"]),
    )
    writer.write_shutdown(h_final)
