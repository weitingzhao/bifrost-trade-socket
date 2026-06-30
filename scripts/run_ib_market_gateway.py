"""Entry point: IB Market Gateway — unified market data edge (W6 trade-k8s-native).

Merges the legacy ingestor + listener + worker_market IB client slots into a single
``market_gateway`` client_id per TWS host. Subscribes watchlist quotes and writes
``ib:ingester:tick:*`` to Redis; Celery historical bars route via ib-operator RPC.

Usage:
  python scripts/run_ib_market_gateway.py
  python scripts/run_ib_market_gateway.py config/config.prod.yaml
  python scripts/run_ib_market_gateway.py --prod
  BIFROST_ENV=stg python scripts/run_ib_market_gateway.py
"""

import logging
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent

from bifrost_socket.ib.ib_timezone_patch import apply_ib_timezone_patch

apply_ib_timezone_patch()

from bifrost_socket.config import IB_MODE_MOCK, get_ib_mode, load_config, resolve_config_path
from bifrost_socket.ib.ingestor.ib_ingestor import IbIngestor


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

    parser = argparse.ArgumentParser(description="IB Market Gateway")
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

    config_path = resolve_config_path(str(_PROJECT_ROOT), sys.argv[1:])
    cfg, resolved = load_config(config_path)

    logging.getLogger(__name__).info("Config loaded: %s", resolved)

    if get_ib_mode(cfg) == IB_MODE_MOCK:
        from bifrost_socket.ib.mock_gateway import run_mock_gateway

        logging.getLogger(__name__).warning(
            "ib.mode=mock — running IB Market Gateway as mock gateway (no TWS socket)"
        )
        run_mock_gateway(cfg, "ib_ingestor")
        return

    from bifrost_socket.ib.lease import run_async_ib_service

    run_async_ib_service(cfg, "ib_market_gateway", lambda: IbIngestor(cfg).run())


if __name__ == "__main__":
    main()
