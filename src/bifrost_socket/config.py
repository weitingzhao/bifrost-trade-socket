"""Shared config loading for bifrost-trade-socket services.

Correct config precedence (B1 fix — legacy checked dev before prod):
  1. BIFROST_CONFIG env var
  2. First positional argument in argv
  3. --prod / --dev / --env prod|dev flag
  4. BIFROST_ENV env var  (default "dev")
  5. config/config.{env}.yaml under project_root
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

_IB_PORT_MAP = {
    "tws_paper": 7497,
    "tws_live": 7496,
    "gateway_paper": 4002,
    "gateway_live": 4001,
}
_VALID_ENVS = ("dev", "prod")


# ── config path resolution ─────────────────────────────────────────────────────

def resolve_config_path(
    project_root: str,
    argv_raw: Optional[List[str]] = None,
) -> str:
    """Resolve config file path with correct env precedence.

    Precedence:
    1. BIFROST_CONFIG env var
    2. First positional argument in argv_raw
    3. --prod / --dev / --env dev|prod flag (consumed from argv_raw)
    4. BIFROST_ENV env var (default "dev")
    5. config/config.{env}.yaml under project_root
    """
    root = Path(project_root)

    if os.environ.get("BIFROST_CONFIG", "").strip():
        p = Path(os.environ["BIFROST_CONFIG"].strip())
        if not p.is_absolute():
            p = root / p
        return str(p.resolve())

    argv = list(argv_raw or [])
    env_from_flag: Optional[str] = None
    positional: Optional[str] = None
    i = 0
    while i < len(argv):
        a = argv[i]
        if a in ("--env", "-e") and i + 1 < len(argv):
            v = argv[i + 1].lower().strip()
            env_from_flag = v if v in _VALID_ENVS else "dev"
            i += 2
            continue
        if a.startswith("--env="):
            v = a.split("=", 1)[1].lower().strip()
            env_from_flag = v if v in _VALID_ENVS else "dev"
            i += 1
            continue
        if a == "--prod":
            env_from_flag = "prod"
            i += 1
            continue
        if a == "--dev":
            env_from_flag = "dev"
            i += 1
            continue
        if not a.startswith("--") and positional is None:
            positional = a
        i += 1

    if positional:
        p = Path(positional)
        if not p.is_absolute():
            p = root / positional
        return str(p.resolve())

    env = (env_from_flag or os.environ.get("BIFROST_ENV", "dev") or "dev").lower().strip()
    if env not in _VALID_ENVS:
        env = "dev"

    candidate = root / "config" / f"config.{env}.yaml"
    if candidate.exists():
        return str(candidate.resolve())

    legacy = root / "config" / "config.yaml"
    if legacy.exists():
        return str(legacy.resolve())

    return str((root / "config" / "config.yaml.example").resolve())


def detect_env(config_path: str) -> str:
    """Return 'dev' or 'prod' based on the loaded file name; empty string for custom paths."""
    name = Path(config_path).name
    if name == "config.dev.yaml":
        return "dev"
    if name == "config.prod.yaml":
        return "prod"
    return ""


# ── YAML loading ───────────────────────────────────────────────────────────────

def _deep_merge(base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merge overlay on top of base (overlay wins)."""
    out = dict(base)
    for k, v in overlay.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def load_config(config_path: str) -> Tuple[Dict[str, Any], str]:
    """Load YAML config, deep-merging config.yaml base when loading dev/prod overlay.

    Returns (config_dict, resolved_absolute_path).
    Stores the resolved path under config["_config_file"] for health hash reporting (B4).
    """
    path = Path(config_path).resolve()
    with open(path, "r", encoding="utf-8") as f:
        overlay: Dict[str, Any] = yaml.safe_load(f) or {}

    if path.name in ("config.dev.yaml", "config.prod.yaml"):
        base_path = path.parent / "config.yaml"
        if base_path.is_file():
            with open(base_path, "r", encoding="utf-8") as f:
                base: Dict[str, Any] = yaml.safe_load(f) or {}
            merged = _deep_merge(base, overlay)
        else:
            merged = overlay
    else:
        merged = overlay

    merged["_config_file"] = str(path)
    return merged, str(path)


# ── IB config extraction ───────────────────────────────────────────────────────

def get_effective_ib_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """Extract IB connection settings from loaded config.

    Returns a flat dict with:
      host, port, port_market_data, connect_timeout,
      client_id_ingestor, client_id_account_agent, client_id_operator,
      client_id_daemon, client_id_listener, client_id_worker_market,
      ib2_host, ib2_port, ib2_client_id_listener, ib2_client_id_operator,
      ib2_client_id_account_agent,
      ib_probe_interval_sec
    """
    ib_raw = config.get("ib")
    if not ib_raw or not isinstance(ib_raw, dict):
        raise ValueError("config['ib'] is required (dict with host.ip, host.port_type, host.client_id)")

    h = ib_raw.get("host")
    if not isinstance(h, dict):
        raise ValueError("config['ib']['host'] is required (dict with ip, port_type, client_id)")

    s = ib_raw.get("secondary") if isinstance(ib_raw.get("secondary"), dict) else {}
    hc = h.get("client_id") if isinstance(h.get("client_id"), dict) else {}
    sc = s.get("client_id") if isinstance(s.get("client_id"), dict) else {}

    host = str(h.get("ip") or "127.0.0.1").strip()
    port_type = str(h.get("port_type") or "tws_paper").strip().lower()
    if port_type not in _IB_PORT_MAP:
        port_type = "tws_paper"
    port = _IB_PORT_MAP[port_type]

    ptp_md = str(h.get("port_type_market_data") or "").strip().lower()
    port_market_data = _IB_PORT_MAP.get(ptp_md, port)

    ib2_host = str(s.get("ip") or "").strip() or None
    ib2_pt = str(s.get("port_type") or "tws_paper").strip().lower()
    ib2_port = _IB_PORT_MAP.get(ib2_pt, _IB_PORT_MAP["tws_paper"])

    return {
        "host": host,
        "port": port,
        "port_market_data": port_market_data,
        "connect_timeout": float(ib_raw.get("connect_timeout") or 60.0),
        "client_id_daemon": int(hc.get("daemon") or 1),
        "client_id_listener": int(hc.get("listener") or 2),
        "client_id_operator": int(hc.get("operator") or hc.get("account") or 100),
        "client_id_worker_market": int(hc.get("worker_market") or 500),
        "client_id_ingestor": int(hc.get("ingestor") or hc.get("ib_market_ingest") or 150),
        "client_id_account_agent": int(hc.get("account_agent") or 151),
        "ib2_host": ib2_host,
        "ib2_port": ib2_port,
        "ib2_client_id_listener": int(sc.get("listener") or 3),
        "ib2_client_id_operator": int(sc.get("operator") or sc.get("account") or 102),
        "ib2_client_id_account_agent": int(sc.get("account_agent") or 152),
        "ib_probe_interval_sec": float(ib_raw.get("probe_interval_sec") or 5.0),
    }


# ── Redis / PostgreSQL factories ───────────────────────────────────────────────

def make_redis_client(config: Dict[str, Any]) -> Any:
    """Create a synchronous redis.Redis client from config['redis']."""
    import redis

    rc = config.get("redis") or {}
    return redis.Redis(
        host=str(rc.get("host") or "127.0.0.1"),
        port=int(rc.get("port") or 6379),
        db=int(rc.get("db") or 0),
        password=rc.get("password") or None,
        socket_connect_timeout=5,
        decode_responses=True,
    )


def get_pg_conn_params(config: Dict[str, Any]) -> Dict[str, Any]:
    """Build psycopg2 connection params from root ``postgres`` block (engine parity)."""
    pg = config.get("postgres") or config.get("database") or {}
    if not isinstance(pg, dict):
        pg = {}
    db = pg.get("database") or pg.get("Database") or pg.get("db") or pg.get("dbname")
    if not db and pg:
        for k, v in pg.items():
            if (
                k
                and isinstance(v, str)
                and v.strip()
                and k.strip().lower() in ("database", "db", "dbname")
            ):
                db = v.strip()
                break
    return {
        "host": str(pg.get("host") or os.environ.get("PGHOST") or "127.0.0.1"),
        "port": int(pg.get("port") or os.environ.get("PGPORT") or 5432),
        "dbname": str(db or os.environ.get("PGDATABASE") or "bifrost_dev"),
        "user": str(pg.get("user") or pg.get("username") or os.environ.get("PGUSER") or "postgres"),
        "password": str(pg.get("password") or os.environ.get("PGPASSWORD") or ""),
        "connect_timeout": 10,
    }
