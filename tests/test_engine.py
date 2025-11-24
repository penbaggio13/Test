"""Lightweight regression ensuring the engine stitches components correctly."""
from __future__ import annotations

import pandas as pd

from backtest_industry.config import StrategyConfig
from backtest_industry.data import MockDataProvider
from backtest_industry.engine import BacktestEngine
from backtest_industry.grid import run_parameter_grid


def _build_mock_daily() -> pd.DataFrame:
    dates = pd.bdate_range("2020-01-01", periods=60)
    rows = []
    specs = {
        "AAA": 1.002,
        "BBB": 1.0005,
        "CCC": 0.999,
    }
    for ts_code, drift in specs.items():
        price = 10.0
        for dt in dates:
            price *= drift
            rows.append(
                {
                    "ts_code": ts_code,
                    "trade_date": dt.strftime("%Y%m%d"),
                    "close": price,
                    "pre_close": price / drift,
                    "adj_factor": 1.0,
                }
            )
    return pd.DataFrame(rows)


def _build_industry_map() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ts_code": ["AAA", "BBB", "CCC"],
            "industry_name": ["通信设备", "通信设备", "白酒"],
        }
    )


def test_backtest_engine_runs_end_to_end() -> None:
    provider = MockDataProvider(_build_mock_daily(), _build_industry_map())
    config = StrategyConfig(
        start_date="2020-01-01",
        end_date="2020-03-31",
        trigger_threshold=-1.0,  # allow every positive delta to trigger
        laggard_pct=0.5,
        hold_days=5,
        top_industry_n=1,
    )
    engine = BacktestEngine(provider, config)
    summary = engine.run()

    assert summary.event_count > 0
    assert summary.avg_return != 0.0
    assert all(evt.tickers for evt in summary.events)
    first = summary.events[0]
    assert first.industries
    assert first.stock_returns
    assert first.laggard_snapshot


def test_parameter_grid_outputs_tables() -> None:
    provider = MockDataProvider(_build_mock_daily(), _build_industry_map())
    config = StrategyConfig(
        start_date="2020-01-01",
        end_date="2020-03-31",
        trigger_threshold=-1.0,
        laggard_pct=0.5,
        hold_days=5,
        top_industry_n=1,
        grid_trigger_thresholds=(0.0, 0.5),
        grid_laggard_pcts=(0.3, 0.5),
    )
    tables = run_parameter_grid(provider, config)
    assert not tables["return_table"].empty
    assert not tables["win_table"].empty
