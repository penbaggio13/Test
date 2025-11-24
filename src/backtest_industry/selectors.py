"""Helpers for picking laggards inside strong industries."""
from __future__ import annotations

import pandas as pd
from typing import List


def pick_laggards(
    weekly_returns: pd.DataFrame,
    industry_map: pd.DataFrame,
    trade_date: pd.Timestamp,
    industries: List[str],
    laggard_pct: float,
) -> pd.DataFrame:
    """Return stocks sitting in the bottom percentile within target industries."""
    if not industries:
        return pd.DataFrame(columns=["ts_code", "industry_name", "weekly_ret"])

    week_slice = weekly_returns[weekly_returns["trade_date"] == trade_date]
    if week_slice.empty:
        return pd.DataFrame(columns=["ts_code", "industry_name", "weekly_ret"])

    merged = week_slice.merge(industry_map, on="ts_code", how="inner")
    laggards = []
    for industry in industries:
        chunk = merged[merged["industry_name"] == industry]
        if chunk.empty:
            continue
        cut = max(1, int(len(chunk) * laggard_pct))
        laggards.append(chunk.nsmallest(cut, "weekly_ret"))
    if not laggards:
        return pd.DataFrame(columns=["ts_code", "industry_name", "weekly_ret"])
    return pd.concat(laggards, ignore_index=True)
