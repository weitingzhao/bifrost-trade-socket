"""Redis keys for IB Account Agent.

Snapshot JSON schema (IB_ACCOUNT_SNAPSHOT_KEY):
  {
    "version": int,
    "updated_at": float,
    "host_connected": bool,
    "secondary_connected": bool,
    "open_orders": [...],
    "accounts_snapshot": [{"account_id", "summary", "positions"}],
    "last_execution_rows": [...]
  }
"""

IB_ACCOUNT_AGENT_HEALTH_KEY = "bifrost:health:ws_ib_account_agent"
IB_ACCOUNT_SNAPSHOT_KEY = "ib:account:snapshot:v1"
IB_ACCOUNT_NOTIFY_CHANNEL = "ib:account:notify"
IB_ACCOUNT_STREAM_KEY = "ib:account:stream:v1"
IB_ACCOUNT_STREAM_MAXLEN = 1000
