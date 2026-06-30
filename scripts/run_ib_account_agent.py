"""Entry point: IB Account Agent — subscribes to account/position updates, writes to Redis.

Usage:
  python scripts/run_ib_account_agent.py
  python scripts/run_ib_account_agent.py config/config.prod.yaml
  python scripts/run_ib_account_agent.py --prod
  python scripts/run_ib_account_agent.py --env dev --log-level DEBUG
  BIFROST_ENV=prod python scripts/run_ib_account_agent.py
"""

import logging
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent

from bifrost_socket.ib.ib_timezone_patch import apply_ib_timezone_patch

apply_ib_timezone_patch()

from bifrost_socket.config import IB_MODE_MOCK, get_ib_mode, load_config, resolve_config_path
from bifrost_socket.ib.account_agent.ib_account_agent import IbAccountAgent


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

    parser = argparse.ArgumentParser(description="IB Account Agent")
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

    if get_ib_mode(cfg) == IB_MODE_MOCK:
        from bifrost_socket.ib.mock_gateway import run_mock_gateway

        logging.getLogger(__name__).warning(
            "ib.mode=mock — running IB Account Agent as mock gateway (no TWS socket)"
        )
        run_mock_gateway(cfg, "ib_account_agent")
        return

    from bifrost_socket.ib.lease import run_async_ib_service

    # W4 trade-k8s-native: gate IB connection behind a K8s Lease (active-standby).
    run_async_ib_service(cfg, "ib_account_agent", lambda: IbAccountAgent(cfg).run())


if __name__ == "__main__":
    main()
