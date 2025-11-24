"""Parameter grid utilities for sensitivity analysis akin to Table 3/4."""
from __future__ import annotations

from dataclasses import replace
from typing import Iterable, Tuple

import pandas as pd

from .config import StrategyConfig
from .data import BaseDataProvider
from .engine import BacktestEngine


def run_parameter_grid(
    provider: BaseDataProvider,
    base_config: StrategyConfig,
    trigger_thresholds: Iterable[float] | None = None,
    laggard_pcts: Iterable[float] | None = None,
) -> dict[str, pd.DataFrame]:
    """Run backtests over a trigger/laggard grid and return summary tables."""
    triggers = list(trigger_thresholds or base_config.grid_trigger_thresholds)
    laggards = list(laggard_pcts or base_config.grid_laggard_pcts)
    results = []
    for trig in triggers:
        for lag in laggards:
            cfg = replace(base_config, trigger_threshold=trig, laggard_pct=lag)
            engine = BacktestEngine(provider, cfg)
            summary = engine.run()
            results.append(
                {
                    "trigger": trig,
                    "laggard": lag,
                    "avg_return": summary.avg_return,
                    "win_rate": summary.win_rate,
                    "events": summary.event_count,
                }
            )
    df = pd.DataFrame(results)
    return {
        "raw": df,
        "return_table": df.pivot(index="trigger", columns="laggard", values="avg_return"),
        "win_table": df.pivot(index="trigger", columns="laggard", values="win_rate"),
    }
