"""Wait for the data layer (CNPG Postgres + Redis) to accept TCP connections.

W7 trade-k8s-native: used as a K8s initContainer so socket/worker pods do not
crashloop before CNPG / Redis (data namespace) are Ready, and as a readiness-probe
exec (``--once``) so a pod goes NotReady — instead of crashing — during data
maintenance.

Endpoints are read from the mounted YAML config (``BIFROST_CONFIG``), so the same
image works across dev/stg/prod without per-overlay env wiring.

Usage:
  python scripts/wait_for_data.py                # block until pg + redis(live) ready
  python scripts/wait_for_data.py --no-pg        # redis only (operator / massive-ws)
  python scripts/wait_for_data.py --queue        # also wait redis_queue (celery/daemon)
  python scripts/wait_for_data.py --once         # single check, exit 0/1 (readiness probe)
  python scripts/wait_for_data.py --timeout 600  # overall deadline (default 600s, 0 = forever)
"""

from __future__ import annotations

import argparse
import os
import socket
import sys
import time
from typing import Any, Dict, List, Tuple

Endpoint = Tuple[str, str, int]  # (label, host, port)


def _config_path() -> str:
    cfg = os.environ.get("BIFROST_CONFIG", "").strip()
    if cfg:
        return cfg
    env = (os.environ.get("BIFROST_ENV", "dev") or "dev").strip().lower()
    return f"/app/config/config.{env}.yaml"


def _load_yaml(path: str) -> Dict[str, Any]:
    import yaml

    with open(path, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    return data if isinstance(data, dict) else {}


def endpoints_from_config(
    cfg: Dict[str, Any],
    *,
    include_pg: bool = True,
    include_queue: bool = False,
) -> List[Endpoint]:
    """Resolve the (label, host, port) tuples to wait for from a loaded config."""
    out: List[Endpoint] = []

    if include_pg:
        pg = cfg.get("postgres") or cfg.get("database") or {}
        if isinstance(pg, dict):
            host = str(pg.get("host") or "").strip()
            if host:
                out.append(("postgres", host, int(pg.get("port") or 5432)))

    redis = cfg.get("redis")
    if isinstance(redis, dict) and redis.get("enabled", True):
        host = str(redis.get("host") or "").strip()
        if host:
            out.append(("redis", host, int(redis.get("port") or 6379)))

    if include_queue:
        rq = cfg.get("redis_queue")
        if isinstance(rq, dict) and rq.get("enabled", True):
            host = str(rq.get("host") or "").strip()
            if host:
                out.append(("redis_queue", host, int(rq.get("port") or 6379)))

    return out


def _tcp_ok(host: str, port: int, connect_timeout: float) -> bool:
    try:
        with socket.create_connection((host, port), timeout=connect_timeout):
            return True
    except OSError:
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Wait for CNPG Postgres + Redis")
    parser.add_argument("--no-pg", action="store_true", help="Skip Postgres (redis-only services)")
    parser.add_argument("--queue", action="store_true", help="Also wait redis_queue")
    parser.add_argument("--once", action="store_true", help="Single check, exit 0/1 (probe mode)")
    parser.add_argument("--timeout", type=float, default=600.0, help="Overall deadline sec (0 = forever)")
    parser.add_argument("--interval", type=float, default=2.0, help="Retry interval sec")
    parser.add_argument("--connect-timeout", type=float, default=3.0, help="Per-attempt TCP timeout sec")
    args = parser.parse_args()

    try:
        cfg = _load_yaml(_config_path())
    except OSError as exc:
        print(f"wait_for_data: cannot read config {_config_path()!r}: {exc}", flush=True)
        return 1

    endpoints = endpoints_from_config(
        cfg, include_pg=not args.no_pg, include_queue=args.queue
    )
    if not endpoints:
        print("wait_for_data: no data endpoints resolved from config — nothing to wait for", flush=True)
        return 0

    deadline = None if args.timeout <= 0 else time.monotonic() + args.timeout
    pending = list(endpoints)
    while True:
        still: List[Endpoint] = []
        for label, host, port in pending:
            if _tcp_ok(host, port, args.connect_timeout):
                print(f"wait_for_data: {label} {host}:{port} ready", flush=True)
            else:
                still.append((label, host, port))
        pending = still
        if not pending:
            print("wait_for_data: all data endpoints ready", flush=True)
            return 0
        if args.once:
            for label, host, port in pending:
                print(f"wait_for_data: {label} {host}:{port} NOT ready", flush=True)
            return 1
        if deadline is not None and time.monotonic() >= deadline:
            for label, host, port in pending:
                print(f"wait_for_data: timeout waiting for {label} {host}:{port}", flush=True)
            return 1
        time.sleep(args.interval)


if __name__ == "__main__":
    sys.exit(main())
