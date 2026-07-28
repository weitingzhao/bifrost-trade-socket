"""PostgreSQL config extraction for socket edge services."""

from __future__ import annotations

from bifrost_socket.config import get_pg_conn_params


def test_get_pg_conn_params_reads_postgres_block() -> None:
    cfg = {
        "postgres": {
            "host": "bifrost-postgres-rw.data.svc.cluster.local",
            "port": 5432,
            "user": "bifrost",
            "password": "secret",
            "database": "bifrost_dev",
        }
    }
    params = get_pg_conn_params(cfg)
    assert params["host"] == "bifrost-postgres-rw.data.svc.cluster.local"
    assert params["port"] == 5432
    assert params["dbname"] == "bifrost_dev"
    assert params["user"] == "bifrost"
    assert params["password"] == "secret"


def test_get_pg_conn_params_does_not_use_root_config_host() -> None:
    """Regression: ib.host must not be mistaken for postgres host."""
    cfg = {
        "ib": {"host": {"ip": "192.168.10.30"}},
        "postgres": {"host": "10.0.0.5", "database": "bifrost_prod"},
    }
    params = get_pg_conn_params(cfg)
    assert params["host"] == "10.0.0.5"
    assert params["dbname"] == "bifrost_prod"
