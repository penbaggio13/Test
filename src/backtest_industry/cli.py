"""Command line hooks for running the backtest end-to-end."""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional, List

import typer
import pandas as pd

from .analytics import export_event_analysis
from .config import StrategyConfig
from .data import TuShareDataProvider
from .engine import BacktestEngine
from .grid import run_parameter_grid

app = typer.Typer(add_completion=False)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


@app.command()
def run(
    start_date: str = typer.Option("2020-01-01", help="Backtest start date"),
    end_date: str = typer.Option("2025-08-31", help="Backtest end date"),
    trigger_threshold: float = typer.Option(0.3, help="Delta concentration trigger"),
    laggard_pct: float = typer.Option(0.3, help="Bottom percentile inside industry"),
    hold_days: int = typer.Option(60, help="Holding period in trading days"),
    top_industry_n: int = typer.Option(3, help="Number of industries to pick"),
    token: Optional[str] = typer.Option(None, help="Override TuShare token"),
    output_prefix: Path = typer.Option(
        Path("outputs/backtest"),
        help="Export analytics to <prefix>_events.csv and <prefix>_summary.json",
    ),
):
    """High-level orchestration command."""

    config = StrategyConfig(
        start_date=start_date,
        end_date=end_date,
        trigger_threshold=trigger_threshold,
        laggard_pct=laggard_pct,
        hold_days=hold_days,
        top_industry_n=top_industry_n,
        tushare_token=token or StrategyConfig().tushare_token,
    )
    provider = TuShareDataProvider(config)
    engine = BacktestEngine(provider, config)
    summary = engine.run()
    analytics_summary = export_event_analysis(summary.events, output_prefix)
    events_path = output_prefix.with_name(f"{output_prefix.stem}_events.csv")
    summary_path = output_prefix.with_name(f"{output_prefix.stem}_summary.json")
    typer.echo(
        json.dumps(
            {
                "events": summary.event_count,
                "avg_return": summary.avg_return,
                "win_rate": summary.win_rate,
                "analytics": analytics_summary,
                "exports": {
                    "events_csv": str(events_path.resolve()),
                    "summary_json": str(summary_path.resolve()),
                },
            },
            ensure_ascii=False,
            indent=2,
        )
    )


@app.command()
def grid(
    start_date: str = typer.Option("2020-01-01", help="Backtest start date"),
    end_date: str = typer.Option("2025-08-31", help="Backtest end date"),
    triggers: Optional[List[float]] = typer.Option(None, help="Trigger thresholds grid"),
    laggards: Optional[List[float]] = typer.Option(None, help="Laggard percent grid"),
    output_prefix: Optional[Path] = typer.Option(
        None, help="If set, export return/win tables to <prefix>_*.csv"
    ),
    token: Optional[str] = typer.Option(None, help="Override TuShare token"),
):
    """Run trigger/laggard sensitivity grid and print tables."""

    config = StrategyConfig(
        start_date=start_date,
        end_date=end_date,
        tushare_token=token or StrategyConfig().tushare_token,
    )
    provider = TuShareDataProvider(config)
    tables = run_parameter_grid(
        provider,
        config,
        trigger_thresholds=triggers,
        laggard_pcts=laggards,
    )
    return_table = tables["return_table"].round(4)
    win_table = tables["win_table"].round(4)
    typer.echo("=== 60D Average Return ===")
    typer.echo(return_table.to_string())
    typer.echo("=== 60D Win Rate ===")
    typer.echo(win_table.to_string())

    if output_prefix:
        output_prefix.parent.mkdir(parents=True, exist_ok=True)
        return_table.to_csv(output_prefix.with_name(f"{output_prefix.stem}_returns.csv"))
        win_table.to_csv(output_prefix.with_name(f"{output_prefix.stem}_winrates.csv"))
        tables["raw"].to_csv(output_prefix.with_name(f"{output_prefix.stem}_raw.csv"), index=False)


@app.command("verify-data")
def verify_data(
    start_date: str = typer.Option("2020-01-01", help="Start date to download"),
    end_date: str = typer.Option("2025-08-31", help="End date to download"),
    industry_level: int = typer.Option(2, min=1, max=3, help="ShenWan hierarchy level"),
    token: Optional[str] = typer.Option(None, help="Override TuShare token"),
):
    """Download行情与行业缓存并打印覆盖范围，便于核查数据完整性。"""

    config = StrategyConfig(
        start_date=start_date,
        end_date=end_date,
        tushare_token=token or StrategyConfig().tushare_token,
    )
    provider = TuShareDataProvider(config)
    daily = provider.get_daily_bars(start_date, end_date)
    industry = provider.get_industry_mapping(level=industry_level)

    trade_dates = pd.to_datetime(daily["trade_date"])
    summary = {
        "cache_dir": str(config.cache_dir.resolve()),
        "daily_rows": int(len(daily)),
        "daily_tickers": int(daily["ts_code"].nunique()),
        "daily_date_range": [
            trade_dates.min().strftime("%Y-%m-%d"),
            trade_dates.max().strftime("%Y-%m-%d"),
        ],
        "industry_rows": int(len(industry)),
        "industry_names": int(industry["industry_name"].nunique()),
        "industry_level": industry_level,
        "chunk_months": config.daily_chunk_months,
    }
    typer.echo(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":  # pragma: no cover
    app()
