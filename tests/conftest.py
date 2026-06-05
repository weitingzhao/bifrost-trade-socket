"""Socket tests: mock Redis client."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest


@pytest.fixture
def mock_redis():
    r = MagicMock()
    r.hgetall.return_value = {}
    r.get.return_value = None
    return r
