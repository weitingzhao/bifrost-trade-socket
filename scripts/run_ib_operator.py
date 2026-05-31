"""Entry point: IB Operator — RPC service for order execution via Redis Streams.

Usage:
  python scripts/run_ib_operator.py
  python scripts/run_ib_operator.py config/config.prod.yaml
  python scripts/run_ib_operator.py --prod
  python scripts/run_ib_operator.py --env dev --log-level DEBUG
  BIFROST_ENV=prod python scripts/run_ib_operator.py

Troubleshooting:
- IB Error 326 / client id in use: assign distinct client_ids in YAML (ib.host.client_id.operator).
- Redis NOGROUP: stream or consumer group was missing; Operator recreates it on startup.
"""

import logging
import signal
import sys
import threading
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent

from bifrost_socket.config import get_effective_ib_config, load_config, resolve_config_path
from bifrost_socket.ib.operator.config import effective_ib_operator_settings
from bifrost_socket.ib.operator.service import run_ib_operator_loop


def _setup_logging(level: int) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s %(name)s [%(levelname)s]  %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
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

    _setup_logging(getattr(logging, args.log_level))
    log = logging.getLogger(__name__)

    # B1 fix: resolve_config_path respects env precedence correctly.
    config_path = resolve_config_path(str(_PROJECT_ROOT), sys.argv[1:])
    cfg, resolved = load_config(config_path)
    log.info("Config loaded: %s", resolved)

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

    run_ib_operator_loop(cfg, stop_event=stop)


if __name__ == "__main__":
    main()
