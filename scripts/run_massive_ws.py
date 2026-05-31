"""Entry point: Massive (Polygon) Options WebSocket ingestor.

Usage:
  python scripts/run_massive_ws.py
  python scripts/run_massive_ws.py config/config.prod.yaml
  python scripts/run_massive_ws.py --prod
  python scripts/run_massive_ws.py --env dev --log-level DEBUG
  BIFROST_ENV=prod python scripts/run_massive_ws.py
"""

import asyncio
import logging
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent

from bifrost_socket.config import load_config, resolve_config_path
from bifrost_socket.massive.massive_ws_ingestor import MassiveWsIngestor, _get_massive_cfg


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

    parser = argparse.ArgumentParser(description="Massive WS Ingestor")
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

    ms = _get_massive_cfg(cfg)
    if not ms["api_key"]:
        log.error("No Massive API key. Set massive.api_key in config or MASSIVE_API_KEY env var.")
        sys.exit(1)

    app = MassiveWsIngestor(cfg)
    asyncio.run(app.run())


if __name__ == "__main__":
    main()
