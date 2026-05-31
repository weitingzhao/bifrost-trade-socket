"""IB Operator main loop: Redis Stream consumer + IB executor."""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from typing import Any, Dict, Optional

import redis

from bifrost_socket.config import get_effective_ib_config, make_redis_client
from bifrost_socket.ib.connector.ib_client import (
    AccountIbClient,
    OperatorIbClient,
    SERVICE_HEARTBEAT_CONNECT_TIMEOUT_SEC,
)
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
from bifrost_socket.config import detect_env

logger = logging.getLogger(__name__)


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
    *,
    service_heartbeat_interval_sec: Optional[float] = None,
    last_service_heartbeat_at: Optional[float] = None,
    service_heartbeat_reconnect_in_progress: Optional[str] = None,
) -> None:
    try:
        asyncio.run(executor.record_ib_probe(probe_interval_sec))
    except Exception as e:
        logger.debug("record_ib_probe: %s", e)
    h = executor.health_dict()
    now = time.time()
    if service_heartbeat_reconnect_in_progress is not None:
        h["service_heartbeat_reconnect_in_progress"] = service_heartbeat_reconnect_in_progress
    if service_heartbeat_interval_sec is not None and last_service_heartbeat_at is not None:
        iv = float(service_heartbeat_interval_sec)
        lh = float(last_service_heartbeat_at)
        h["service_heartbeat_interval_sec"] = iv
        h["last_service_heartbeat_at"] = lh
        h["next_service_heartbeat_in_s"] = max(0.0, lh + iv - now) if lh > 0 else iv
    writer.write(h)


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
    writer = IbOperatorHealthWriter(r, env=env, config_file=config_file)

    stream = settings["stream"]
    group = settings["consumer_group"]
    cons = consumer_name()
    block_ms = settings["block_ms"]
    result_ttl = settings["result_ttl_sec"]
    result_prefix = settings["result_prefix"]
    max_bytes = settings["max_result_bytes"]
    health_refresh = float(settings["health_refresh_sec"])

    ib_cfg = get_effective_ib_config(config)
    probe_iv = float(ib_cfg["ib_probe_interval_sec"])
    health_refresh = max(health_refresh, probe_iv)

    ensure_stream_and_group(r, stream, group)
    executor = _build_executor(config)
    runner = OperatorRedisRunner(r, stream, group, cons, block_ms)

    logger.info("IB Operator started stream=%s group=%s consumer=%s", stream, group, cons)

    last_hb_at = time.time()
    _write_health(
        writer, executor, probe_iv,
        service_heartbeat_interval_sec=health_refresh,
        last_service_heartbeat_at=last_hb_at,
    )

    try:
        asyncio.run(executor.connect_all())
    except Exception as e:
        logger.warning("IB Operator initial connect failed (will retry on heartbeat): %s", e)

    _write_health(
        writer, executor, probe_iv,
        service_heartbeat_interval_sec=health_refresh,
        last_service_heartbeat_at=last_hb_at,
    )

    stop = stop_event or threading.Event()

    while not stop.is_set():
        now = time.time()
        if now - last_hb_at >= health_refresh:
            # Service heartbeat: one reconnect attempt per slot; failures wait for next tick.
            hd = executor.health_dict()
            host_ok = bool((hd.get("host") or {}).get("connected"))
            if not host_ok:
                _hid = int((hd.get("host") or {}).get("client_id") or 0)
                _write_health(
                    writer, executor, probe_iv,
                    service_heartbeat_interval_sec=health_refresh,
                    last_service_heartbeat_at=last_hb_at,
                    service_heartbeat_reconnect_in_progress=f"Host (client {_hid})",
                )
                async def _hb_host() -> None:
                    await asyncio.wait_for(
                        executor.connect_primary_only(max_connect_attempts=1),
                        timeout=SERVICE_HEARTBEAT_CONNECT_TIMEOUT_SEC,
                    )
                try:
                    asyncio.run(_hb_host())
                except asyncio.TimeoutError:
                    logger.debug(
                        "IB Operator heartbeat Host timed out after %.0fs",
                        SERVICE_HEARTBEAT_CONNECT_TIMEOUT_SEC,
                    )
                except Exception as e:
                    logger.debug("IB Operator heartbeat Host: %s", e)
                finally:
                    _write_health(
                        writer, executor, probe_iv,
                        service_heartbeat_interval_sec=health_refresh,
                        last_service_heartbeat_at=last_hb_at,
                        service_heartbeat_reconnect_in_progress="",
                    )

            hd_sec = executor.health_dict()
            sec = hd_sec.get("secondary")
            if isinstance(sec, dict) and not bool(sec.get("connected")):
                _sid = int(sec.get("client_id") or 0)
                _write_health(
                    writer, executor, probe_iv,
                    service_heartbeat_interval_sec=health_refresh,
                    last_service_heartbeat_at=last_hb_at,
                    service_heartbeat_reconnect_in_progress=f"Secondary (client {_sid})",
                )
                async def _hb_secondary() -> None:
                    await asyncio.wait_for(
                        executor.connect_secondary_only(max_connect_attempts=1),
                        timeout=SERVICE_HEARTBEAT_CONNECT_TIMEOUT_SEC,
                    )
                try:
                    asyncio.run(_hb_secondary())
                except asyncio.TimeoutError:
                    logger.debug(
                        "IB Operator heartbeat Secondary timed out after %.0fs",
                        SERVICE_HEARTBEAT_CONNECT_TIMEOUT_SEC,
                    )
                except Exception as e:
                    logger.debug("IB Operator heartbeat Secondary: %s", e)
                finally:
                    _write_health(
                        writer, executor, probe_iv,
                        service_heartbeat_interval_sec=health_refresh,
                        last_service_heartbeat_at=last_hb_at,
                        service_heartbeat_reconnect_in_progress="",
                    )

            last_hb_at = now
            _write_health(
                writer, executor, probe_iv,
                service_heartbeat_interval_sec=health_refresh,
                last_service_heartbeat_at=last_hb_at,
                service_heartbeat_reconnect_in_progress="",
            )

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
            entries = runner.read_new()

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

            outcome = asyncio.run(_handle_message(executor, msg))
            body, enc_err = dumps_result(outcome, max_bytes=max_bytes)
            if enc_err:
                body, _ = dumps_result({"ok": False, "error": enc_err}, max_bytes=max_bytes)
            write_result(r, rk, body or '{"ok":false,"error":"encode"}', ttl_sec=result_ttl)
            ack_message(r, stream, group, entry_id)
            executor.note_cmd_processed()
            _write_health(
                writer, executor, probe_iv,
                service_heartbeat_interval_sec=health_refresh,
                last_service_heartbeat_at=last_hb_at,
            )

    logger.info("IB Operator stopping: disconnecting IB clients")
    try:
        asyncio.run(executor.disconnect_all())
    except Exception as e:
        logger.warning("disconnect on shutdown: %s", e)
    h_final = executor.health_dict()
    writer.write_shutdown(h_final)
