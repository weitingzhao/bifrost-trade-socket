"""Entry point: IB Operator — read-only TWS RPC via Redis Streams (bars, options, account snapshots).

Usage:
  python scripts/run_ib_operator.py
  python scripts/run_ib_operator.py config/config.prod.yaml
  python scripts/run_ib_operator.py --prod
  python scripts/run_ib_operator.py --env dev --log-level DEBUG
  BIFROST_ENV=prod python scripts/run_ib_operator.py

Troubleshooting:
- IB Error 326 / client id in use: assign distinct client_ids in YAML (ib.host.client_id.operator).
- Redis NOGROUP: stream or consumer group was missing; Operator recreates it on startup.
- Dashboard log: Redis stream ``bifrost:console:ws_ib_operator`` (Monitor IB Operator log panel).
"""

import logging
import signal
import sys
import threading
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Must run before any import chain that loads ib_insync.decoder (binds parseIBDatetime).
from bifrost_socket.ib.ib_timezone_patch import apply_ib_timezone_patch

apply_ib_timezone_patch()

from bifrost_socket.config import (
    IB_MODE_MOCK,
    get_effective_ib_config,
    get_ib_mode,
    load_config,
    resolve_config_path,
)
from bifrost_socket.ib.operator.config import effective_ib_operator_settings
from bifrost_socket.ib.operator.redis_keys import IB_OPERATOR_LOG_STREAM_KEY
from bifrost_socket.ib.operator.service import run_ib_operator_loop

_LOG_STREAM_MAXLEN = 2000


def _console_log_redis_url(config: dict) -> str:
    from bifrost_core.core.redis_url import effective_redis_dict, format_redis_url

    return format_redis_url(effective_redis_dict(config, default_db=0))


def _setup_logging(level: int, config: dict) -> None:
    from bifrost_core.core.logging_redis_stream import RedisStreamLogHandler

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s %(name)s [%(levelname)s]  %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    redis_handler = RedisStreamLogHandler(
        _console_log_redis_url(config),
        IB_OPERATOR_LOG_STREAM_KEY,
        maxlen=_LOG_STREAM_MAXLEN,
    )
    redis_handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(console_handler)
    root.addHandler(redis_handler)
    root.setLevel(level)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="IB Operator")
    parser.add_argument("config", nargs="?", help="Path to YAML config (optional)")
    parser.add_argument("--prod", action="store_true", help="Use config/config.prod.yaml")
    parser.add_argument("--dev", action="store_true", help="Use config/config.dev.yaml")
    parser.add_argument("--env", choices=["dev", "prod"], default=None)
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    args = parser.parse_args()

    config_path = resolve_config_path(str(_PROJECT_ROOT), sys.argv[1:])
    cfg, resolved = load_config(config_path)
    _setup_logging(getattr(logging, args.log_level), cfg)
    log = logging.getLogger(__name__)

    log.info("Config loaded: %s", resolved)

    if get_ib_mode(cfg) == IB_MODE_MOCK:
        from bifrost_socket.ib.mock_gateway import run_mock_gateway

        log.warning("ib.mode=mock — running IB Operator as mock gateway (no TWS socket)")
        run_mock_gateway(cfg, "ib_operator")
        return

    op = effective_ib_operator_settings(cfg)
    log.info("IB Operator Redis health hash: %s", op["health_key"])
    if not op["enabled"]:
        log.error("IB Operator disabled (set ib_operator.enabled: true and enable Redis).")
        sys.exit(1)
    try:
        get_effective_ib_config(cfg)
    except ValueError as e:
        log.error("Invalid or missing ib: block: %s", e)
        sys.exit(1)

    stop = threading.Event()

    def _handle_sig(_signum: int, _frame: object) -> None:
        log.info("Signal received, stopping...")
        stop.set()

    signal.signal(signal.SIGTERM, _handle_sig)
    signal.signal(signal.SIGINT, _handle_sig)

    from bifrost_socket.ib.lease import run_sync_ib_service

    # W4 trade-k8s-native: gate the IB Operator socket behind a K8s Lease.
    # When the Lease is lost the inner stop event unwinds the operator loop.
    run_sync_ib_service(
        cfg,
        "ib_operator",
        lambda inner_stop: run_ib_operator_loop(cfg, stop_event=inner_stop),
        stop,
    )


if __name__ == "__main__":
    main()
