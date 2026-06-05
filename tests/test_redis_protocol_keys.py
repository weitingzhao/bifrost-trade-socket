"""Redis protocol key parity with engine."""

from bifrost_core.core.realtime.ib_ingestor_keys import (
    IB_INGESTER_TICK_PREFIX,
    IB_INGESTER_PREFIX,
)
from bifrost_core.core.realtime.ib_account_keys import IB_ACCOUNT_SNAPSHOT_KEY


def test_ib_ingestor_tick_prefix():
    assert IB_INGESTER_TICK_PREFIX == "ib:ingester:tick:"
    assert IB_INGESTER_PREFIX == "ib:ingester"


def test_ib_account_snapshot_key():
    assert IB_ACCOUNT_SNAPSHOT_KEY == "ib:account:snapshot:v1"
