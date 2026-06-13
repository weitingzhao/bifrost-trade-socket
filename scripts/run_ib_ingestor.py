"""Entry point: IB Ingestor — subscribes to IB market data, writes to Redis.

Usage:
  python scripts/run_ib_ingestor.py
  python scripts/run_ib_ingestor.py config/config.prod.yaml
  python scripts/run_ib_ingestor.py --prod
  python scripts/run_ib_ingestor.py --env dev --log-level DEBUG
  BIFROST_ENV=prod python scripts/run_ib_ingestor.py
"""

import asyncio
import logging
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Before ib_insync.decoder loads (binds parseIBDatetime for execDetails / US/Eastern).
from bifrost_socket.ib.ib_timezone_patch import apply_ib_timezone_patch

apply_ib_timezone_patch()

from bifrost_socket.config import load_config, resolve_config_path
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

    parser = argparse.ArgumentParser(description="IB Ingestor")
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

    # B1 fix: resolve_config_path respects env precedence correctly.
    config_path = resolve_config_path(str(_PROJECT_ROOT), sys.argv[1:])
    cfg, resolved = load_config(config_path)

    logging.getLogger(__name__).info("Config loaded: %s", resolved)

    app = IbIngestor(cfg)
    asyncio.run(app.run())


if __name__ == "__main__":
    main()
