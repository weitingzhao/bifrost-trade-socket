"""Unit tests for scripts/wait_for_data.py endpoint resolution (W7 trade-k8s-native)."""

from __future__ import annotations

import importlib.util
from pathlib import Path

_MOD_PATH = Path(__file__).resolve().parent.parent / "scripts" / "wait_for_data.py"
_spec = importlib.util.spec_from_file_location("wait_for_data", _MOD_PATH)
wait_for_data = importlib.util.module_from_spec(_spec)
assert _spec and _spec.loader
_spec.loader.exec_module(wait_for_data)


_CFG = {
    "postgres": {"host": "bifrost-postgres-rw.data.svc.cluster.local", "port": 5432},
    "redis": {"enabled": True, "host": "redis-live-stg.data.svc.cluster.local", "port": 6379},
    "redis_queue": {"enabled": True, "host": "redis-queue-stg.data.svc.cluster.local", "port": 6379},
}


def test_default_waits_pg_and_redis_live():
    eps = wait_for_data.endpoints_from_config(_CFG)
    labels = [e[0] for e in eps]
    assert labels == ["postgres", "redis"]
    assert ("redis", "redis-live-stg.data.svc.cluster.local", 6379) in eps


def test_no_pg_skips_postgres():
    eps = wait_for_data.endpoints_from_config(_CFG, include_pg=False)
    labels = [e[0] for e in eps]
    assert "postgres" not in labels
    assert labels == ["redis"]


def test_queue_includes_redis_queue():
    eps = wait_for_data.endpoints_from_config(_CFG, include_queue=True)
    labels = [e[0] for e in eps]
    assert labels == ["postgres", "redis", "redis_queue"]


def test_disabled_redis_is_skipped():
    cfg = {"postgres": {"host": "pg", "port": 5432}, "redis": {"enabled": False, "host": "r"}}
    eps = wait_for_data.endpoints_from_config(cfg)
    assert [e[0] for e in eps] == ["postgres"]


def test_missing_blocks_resolve_to_empty():
    assert wait_for_data.endpoints_from_config({}, include_pg=True, include_queue=True) == []
