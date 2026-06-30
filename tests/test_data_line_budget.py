"""Tests for IB data-line budget ConfigMap loader (W10)."""

from __future__ import annotations

from pathlib import Path

import pytest

from bifrost_socket.ib.data_line_budget import (
    IbDataLineBudget,
    load_ib_data_line_budget,
    resolve_gateway_max_subscriptions,
)


def test_defaults_when_mount_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("IB_DATA_LINE_BUDGET_DIR", raising=False)
    budget = load_ib_data_line_budget(budget_dir=str(tmp_path / "missing"))
    assert budget.account_budget == 100
    assert budget.gateway_max_subscriptions == 200
    assert budget.mounted is False


def test_reads_configmap_files(tmp_path: Path) -> None:
    (tmp_path / "account_budget").write_text("90\n", encoding="utf-8")
    (tmp_path / "gateway_max_subscriptions").write_text("150\n", encoding="utf-8")
    (tmp_path / "reserved_tws_ui_lines").write_text("5\n", encoding="utf-8")
    budget = load_ib_data_line_budget(budget_dir=str(tmp_path))
    assert budget.mounted is True
    assert budget.account_budget == 90
    assert budget.gateway_max_subscriptions == 150
    assert budget.reserved_tws_ui_lines == 5


def test_mount_wins_over_yaml_cap() -> None:
    mounted = IbDataLineBudget(
        account_budget=100,
        gateway_max_subscriptions=120,
        reserved_tws_ui_lines=10,
        mounted=True,
    )
    assert resolve_gateway_max_subscriptions(mounted, config_value=200) == 120
    defaults = IbDataLineBudget(100, 200, 10, mounted=False)
    assert resolve_gateway_max_subscriptions(defaults, config_value=180) == 180
