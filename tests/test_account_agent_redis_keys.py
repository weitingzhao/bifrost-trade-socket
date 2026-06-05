"""IB account agent Redis keys."""

from bifrost_socket.ib.account_agent.redis_keys import IB_ACCOUNT_AGENT_HEALTH_KEY


def test_account_agent_health_key() -> None:
    assert IB_ACCOUNT_AGENT_HEALTH_KEY == "bifrost:health:ws_ib_account_agent"
