"""Socket tests: mock Redis client."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

_CORE_CONFIG_EXAMPLE = (
    Path(__file__).resolve().parents[2] / "bifrost-trade-core" / "config" / "config.yaml.example"
)


@pytest.fixture(autouse=True)
def bifrost_config_path(monkeypatch: pytest.MonkeyPatch) -> None:
    if _CORE_CONFIG_EXAMPLE.is_file():
        monkeypatch.setenv("BIFROST_CONFIG", str(_CORE_CONFIG_EXAMPLE))


@pytest.fixture
def mock_redis():
    r = MagicMock()
    r.hgetall.return_value = {}
    r.get.return_value = None
    return r
