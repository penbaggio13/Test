"""Per-event analytics helpers for explaining backtest outputs."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping

import pandas as pd

from .engine import EventResult


def _empty_event_table() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "event_id",
            "signal_date",
            "entry_date",
            "exit_date",
            "holding_days",
            "industries",
            "ticker_count",
            "tickers",
            "total_return",
            "max_drawdown",
            "win",
        ]
    )


def build_event_table(events: Iterable[EventResult]) -> pd.DataFrame:
    """Convert EventResult objects into a tabular structure with derived stats."""

    records: list[Dict[str, Any]] = []
    for idx, evt in enumerate(events):
        equity_curve = (1 + evt.path).cumprod()
        if equity_curve.empty:
            max_drawdown = 0.0
            holding_days = 0
        else:
            # Drawdown uses running peak to highlight worst path dip per event.
            drawdowns = equity_curve.divide(equity_curve.cummax()).sub(1.0)
            max_drawdown = float(drawdowns.min())
            holding_days = len(equity_curve)
        records.append(
            {
                "event_id": idx,
                "signal_date": evt.trade_date,
                "entry_date": evt.entry_date,
                "exit_date": evt.exit_date,
                "holding_days": holding_days,
                "industries": ",".join(evt.industries),
                "ticker_count": len(evt.tickers),
                "tickers": ",".join(evt.tickers),
                "total_return": float(evt.total_return),
                "max_drawdown": max_drawdown,
                "win": bool(evt.total_return > 0),
            }
        )
    if not records:
        return _empty_event_table()

    table = pd.DataFrame.from_records(records)
    table.sort_values("entry_date", inplace=True)
    table.reset_index(drop=True, inplace=True)
    return table


def summarize_quantiles(
    event_table: pd.DataFrame, percentiles: Iterable[float] | None = None
) -> Dict[str, float]:
    if percentiles is None:
        percentiles = (0.1, 0.25, 0.5, 0.75, 0.9)
    if event_table.empty:
        return {f"p{int(p * 100)}": 0.0 for p in percentiles}
    return {
        f"p{int(p * 100)}": float(event_table["total_return"].quantile(p))
        for p in percentiles
    }


def summarize_by_year(event_table: pd.DataFrame) -> pd.DataFrame:
    if event_table.empty:
        return pd.DataFrame(
            columns=["year", "events", "avg_return", "median_return", "win_rate", "avg_drawdown"]
        )

    table = event_table.copy()
    table["year"] = pd.to_datetime(table["entry_date"]).dt.year
    grouped = (
        table.groupby("year")
        .agg(
            events=("event_id", "count"),
            avg_return=("total_return", "mean"),
            median_return=("total_return", "median"),
            win_rate=("win", "mean"),
            avg_drawdown=("max_drawdown", "mean"),
        )
        .reset_index()
        .sort_values("year")
    )
    return grouped


def _build_summary_payload(event_table: pd.DataFrame) -> Dict[str, Any]:
    if event_table.empty:
        return {
            "event_count": 0,
            "avg_return": 0.0,
            "win_rate": 0.0,
            "quantiles": summarize_quantiles(event_table),
            "yearly": [],
        }

    yearly = summarize_by_year(event_table)
    return {
        "event_count": int(len(event_table)),
        "avg_return": float(event_table["total_return"].mean()),
        "win_rate": float(event_table["win"].mean()),
        "quantiles": summarize_quantiles(event_table),
        "yearly": yearly.to_dict(orient="records"),
    }


def export_event_analysis(
    events: Iterable[EventResult], output_prefix: Path
) -> Mapping[str, Any]:
    """Dump detailed analytics to CSV/JSON files."""

    event_table = build_event_table(events)
    summary = _build_summary_payload(event_table)

    output_prefix = Path(output_prefix)
    output_prefix.parent.mkdir(parents=True, exist_ok=True)
    events_path = output_prefix.with_name(f"{output_prefix.stem}_events.csv")
    summary_path = output_prefix.with_name(f"{output_prefix.stem}_summary.json")

    event_table.to_csv(events_path, index=False)
    with summary_path.open("w", encoding="utf-8") as fh:
        json.dump(summary, fh, ensure_ascii=False, indent=2)

    return summary
