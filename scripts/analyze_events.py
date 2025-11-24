"""Utility script to visualize per-event backtest outputs."""
from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


def _load_events(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    date_cols = ["signal_date", "entry_date", "exit_date"]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col])
    df["year"] = df["entry_date"].dt.year
    return df


def plot_return_distribution(events: pd.DataFrame, output_path: Path) -> None:
    plt.figure(figsize=(8, 5))
    plt.hist(events["total_return"], bins=20, color="#4C72B0", edgecolor="black")
    plt.axvline(events["total_return"].mean(), color="red", linestyle="--", label="Mean")
    plt.title("Event Total Return Distribution")
    plt.xlabel("Total Return")
    plt.ylabel("Event Count")
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()


def plot_yearly_performance(events: pd.DataFrame, output_path: Path) -> pd.DataFrame:
    grouped = (
        events.groupby("year")
        .agg(
            event_count=("event_id", "count"),
            avg_return=("total_return", "mean"),
            median_return=("total_return", "median"),
            win_rate=("win", "mean"),
        )
        .reset_index()
        .sort_values("year")
    )

    plt.figure(figsize=(9, 5))
    bars = plt.bar(grouped["year"], grouped["avg_return"], color="#55A868")
    plt.title("Average Event Return by Year")
    plt.xlabel("Entry Year")
    plt.ylabel("Average Total Return")
    plt.axhline(0, color="black", linewidth=0.8)
    for bar, count in zip(bars, grouped["event_count"]):
        plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), f"n={count}", ha="center", va="bottom", fontsize=8)
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()
    return grouped


def export_top_bottom(events: pd.DataFrame, top_n: int, output_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    top_events = events.nlargest(top_n, "total_return")
    bottom_events = events.nsmallest(top_n, "total_return")
    top_events.to_csv(output_dir / "top_events.csv", index=False)
    bottom_events.to_csv(output_dir / "bottom_events.csv", index=False)
    return top_events, bottom_events


def main() -> None:
    parser = argparse.ArgumentParser(description="Plot summaries for per-event backtest CSV outputs.")
    parser.add_argument("--csv", required=True, type=Path, help="Path to the *_events.csv file")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory to store plots and filtered tables (default: <csv_dir>/analysis)",
    )
    parser.add_argument("--top-n", type=int, default=5, help="How many best/worst events to export")
    args = parser.parse_args()

    csv_path: Path = args.csv
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    output_dir = args.output_dir or (csv_path.parent / "analysis")
    output_dir.mkdir(parents=True, exist_ok=True)

    events = _load_events(csv_path)

    histogram_path = output_dir / "return_distribution.png"
    yearly_chart_path = output_dir / "yearly_avg_return.png"

    plot_return_distribution(events, histogram_path)
    yearly_summary = plot_yearly_performance(events, yearly_chart_path)
    top_events, bottom_events = export_top_bottom(events, args.top_n, output_dir)

    print("Saved artifacts:")
    print(f"  Histogram: {histogram_path}")
    print(f"  Yearly bar chart: {yearly_chart_path}")
    print(f"  Top {args.top_n} events: {output_dir / 'top_events.csv'}")
    print(f"  Bottom {args.top_n} events: {output_dir / 'bottom_events.csv'}")
    print("Yearly summary (avg return / win rate):")
    print(yearly_summary[["year", "event_count", "avg_return", "win_rate"]].to_string(index=False))
    print("Top events preview:")
    print(top_events[["event_id", "entry_date", "total_return", "max_drawdown", "industries"]].to_string(index=False))
    print("Bottom events preview:")
    print(bottom_events[["event_id", "entry_date", "total_return", "max_drawdown", "industries"]].to_string(index=False))


if __name__ == "__main__":
    main()
