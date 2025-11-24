"""Centralized configuration utilities for the industry backtest."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import os
from typing import Tuple

DEFAULT_TUSHARE_TOKEN = os.getenv(
    "TUSHARE_TOKEN",
    "98b2900883e70c8b1e141fdb33e4a5a1123dc999d217fcd2c0ce4c89",
)


@dataclass(slots=True)
class StrategyConfig:
    """Bundle all tunable knobs for the pure-industry workflow."""

    start_date: str = "2020-01-01"
    end_date: str = "2025-08-31"
    market_top_pct: float = 0.3
    industry_top_pct: float = 0.3
    laggard_pct: float = 0.3
    trigger_threshold: float = 0.3
    hold_days: int = 60
    top_industry_n: int = 3
    calendar: str = "SSE"
    tushare_token: str = DEFAULT_TUSHARE_TOKEN
    cache_dir: Path = field(default_factory=lambda: Path("data_cache"))
    weekly_resample_rule: str = "W-FRI"
    sw_src: str = "SW2021"
    sw_is_new: str = "Y"
    daily_chunk_months: int = 3
    daily_row_limit: int = 5500
    grid_trigger_thresholds: Tuple[float, ...] = (0.0, 0.1, 0.3, 0.5)
    grid_laggard_pcts: Tuple[float, ...] = (0.3, 0.5, 0.7)

    def ensure_cache_dir(self) -> Path:
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        return self.cache_dir


def resolve_dates(config: StrategyConfig) -> Tuple[str, str]:
    return config.start_date, config.end_date
