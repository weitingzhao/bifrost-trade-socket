"""IB market data line budget — K8s ConfigMap mount (W10 trade-k8s-native).

TWS shares an account-level market data line budget (~100 default) across the TWS UI
and every API client. The ib-market-gateway is the sole subscriber per environment;
readers consume Redis only.

Mount ``ib-data-line-budget`` ConfigMap at ``IB_DATA_LINE_BUDGET_DIR`` (default
``/etc/bifrost/ib-data-line-budget``) with keys:
  account_budget, gateway_max_subscriptions, reserved_tws_ui_lines
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

_DEFAULT_DIR = "/etc/bifrost/ib-data-line-budget"


@dataclass(frozen=True)
class IbDataLineBudget:
    account_budget: int
    gateway_max_subscriptions: int
    reserved_tws_ui_lines: int
    mounted: bool


def _read_int_file(base: Path, name: str, default: int) -> int:
    path = base / name
    if not path.is_file():
        return default
    try:
        return max(0, int(path.read_text(encoding="utf-8").strip()))
    except (OSError, ValueError):
        return default


def load_ib_data_line_budget(
    *,
    budget_dir: str | None = None,
) -> IbDataLineBudget:
    """Load budget from ConfigMap mount when present; otherwise return K8s-friendly defaults."""
    base = Path(budget_dir or os.environ.get("IB_DATA_LINE_BUDGET_DIR") or _DEFAULT_DIR)
    mounted = base.is_dir() and any(base.iterdir()) if base.exists() else False
    return IbDataLineBudget(
        account_budget=_read_int_file(base, "account_budget", 100),
        gateway_max_subscriptions=_read_int_file(base, "gateway_max_subscriptions", 200),
        reserved_tws_ui_lines=_read_int_file(base, "reserved_tws_ui_lines", 10),
        mounted=mounted,
    )


def resolve_gateway_max_subscriptions(
    budget: IbDataLineBudget,
    config_value: int | None,
) -> int:
    """ConfigMap mount wins when present; else YAML ib_ingestor.max_subscriptions."""
    if budget.mounted:
        return max(1, min(5000, budget.gateway_max_subscriptions))
    if config_value is not None:
        return max(1, min(5000, config_value))
    return 200
