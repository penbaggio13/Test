"""All concentration-related computations (market + industry)."""
from __future__ import annotations

from typing import Tuple
import pandas as pd
import numpy as np


def build_daily_returns(daily_bars: pd.DataFrame) -> pd.DataFrame:
    """Derive daily percentage returns leveraging adj close."""
    df = daily_bars.copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df.sort_values(["ts_code", "trade_date"], inplace=True)
    df["adj_close"] = df["close"] * df["adj_factor"].div(df.groupby("ts_code")["adj_factor"].shift(1))
    df["adj_close"] = df.groupby("ts_code")["adj_close"].ffill()
    df["ret"] = df.groupby("ts_code")["adj_close"].pct_change(fill_method=None)
    return df.dropna(subset=["ret"])[["ts_code", "trade_date", "ret"]]


def resample_weekly_returns(
    daily_returns: pd.DataFrame,
    rule: str,
) -> pd.DataFrame:
    """Resample daily returns to weekly compounded returns."""
    frames: list[pd.DataFrame] = []
    for ts_code, grp in daily_returns.groupby("ts_code"):
        ts_grp = grp.set_index("trade_date")
        weekly = (1 + ts_grp["ret"]).resample(rule).apply(lambda x: (1 + x).prod() - 1)
        weekly = weekly.dropna().reset_index()
        weekly["ts_code"] = ts_code
        weekly.rename(columns={"ret": "weekly_ret"}, inplace=True)
        frames.append(weekly)
    if not frames:
        return pd.DataFrame(columns=["trade_date", "weekly_ret", "ts_code"])
    weekly_returns = pd.concat(frames, ignore_index=True)
    return weekly_returns[["ts_code", "trade_date", "weekly_ret"]]


def _top_mean(series: pd.Series, quantile: float) -> float:
    cut = max(1, int(np.ceil(len(series) * quantile)))
    return series.nlargest(cut).mean()


def _bottom_mean(series: pd.Series, quantile: float) -> float:
    cut = max(1, int(np.ceil(len(series) * quantile)))
    return series.nsmallest(cut).mean()


def compute_market_concentration(
    weekly_returns: pd.DataFrame,
    top_quantile: float,
) -> pd.DataFrame:
    """Compute market-wide concentration and its weekly delta."""
    grouped = weekly_returns.groupby("trade_date")["weekly_ret"]
    top = grouped.apply(lambda x: _top_mean(x, top_quantile))
    median = grouped.median()
    df = pd.DataFrame({"top_mean": top, "median": median})
    df["concentration"] = df["top_mean"] - df["median"]
    df["delta"] = df["concentration"].diff()
    return df.reset_index()


def compute_industry_concentration(
    weekly_returns: pd.DataFrame,
    industry_map: pd.DataFrame,
    top_quantile: float,
) -> pd.DataFrame:
    """Compute concentration for each industry every week."""
    merged = weekly_returns.merge(industry_map, on="ts_code", how="inner")
    grouped = merged.groupby(["trade_date", "industry_name"])["weekly_ret"]
    top = grouped.apply(lambda x: _top_mean(x, top_quantile))
    median = grouped.median()
    df = pd.DataFrame({"top_mean": top, "median": median})
    df["concentration"] = df["top_mean"] - df["median"]
    return df.reset_index()


def rank_industries(
    industry_concentration: pd.DataFrame,
    trade_date: pd.Timestamp,
    top_n: int,
) -> Tuple[pd.Timestamp, list[str]]:
    """Return the top-N industries by concentration on a given week."""
    slice_ = industry_concentration[industry_concentration["trade_date"] == trade_date]
    if slice_.empty:
        return trade_date, []
    ordered = slice_.nlargest(top_n, "concentration")
    return trade_date, ordered["industry_name"].tolist()
