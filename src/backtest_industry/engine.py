"""Core backtest loop that wires data + signals + event evaluation."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Dict, Tuple
import logging

import pandas as pd

from .config import StrategyConfig
from .data import BaseDataProvider
from . import concentration, selectors

_LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class EventResult:
    trade_date: pd.Timestamp
    entry_date: pd.Timestamp
    exit_date: pd.Timestamp
    tickers: List[str]
    total_return: float
    path: pd.Series
    industries: List[str]
    stock_returns: Dict[str, float]
    laggard_snapshot: List[Dict[str, object]]


@dataclass(slots=True)
class BacktestSummary:
    events: List[EventResult]

    @property
    def event_count(self) -> int:
        return len(self.events)

    @property
    def avg_return(self) -> float:
        if not self.events:
            return 0.0
        return float(sum(evt.total_return for evt in self.events) / len(self.events))

    @property
    def win_rate(self) -> float:
        if not self.events:
            return 0.0
        wins = sum(1 for evt in self.events if evt.total_return > 0)
        return wins / len(self.events)


class BacktestEngine:
    """Execute the pure-industry event strategy."""

    def __init__(self, data_provider: BaseDataProvider, config: StrategyConfig) -> None:
        self.data_provider = data_provider
        self.config = config

    def run(self) -> BacktestSummary:
        start_date, end_date = self.config.start_date, self.config.end_date
        _LOGGER.info("Loading daily bars between %s and %s", start_date, end_date)
        daily_bars = self.data_provider.get_daily_bars(start_date, end_date)
        industry_map = self.data_provider.get_industry_mapping(level=2)

        daily_returns = concentration.build_daily_returns(daily_bars)
        weekly_returns = concentration.resample_weekly_returns(
            daily_returns, self.config.weekly_resample_rule
        )
        market_conc = concentration.compute_market_concentration(
            weekly_returns, self.config.market_top_pct
        )
        industry_conc = concentration.compute_industry_concentration(
            weekly_returns, industry_map, self.config.industry_top_pct
        )

        events = self._iterate_events(
            market_conc, weekly_returns, industry_conc, industry_map, daily_returns
        )
        return BacktestSummary(events=events)

    # --------------------- Internals ---------------------
    def _iterate_events(
        self,
        market_conc: pd.DataFrame,
        weekly_returns: pd.DataFrame,
        industry_conc: pd.DataFrame,
        industry_map: pd.DataFrame,
        daily_returns: pd.DataFrame,
    ) -> List[EventResult]:
        trading_calendar = daily_returns["trade_date"].sort_values().unique()
        events: List[EventResult] = []
        for _, row in market_conc.iterrows():
            trade_date = row["trade_date"]
            delta = row["delta"]
            if pd.isna(delta) or delta <= self.config.trigger_threshold:
                continue
            _, industries = concentration.rank_industries(
                industry_conc, trade_date, self.config.top_industry_n
            )
            laggards = selectors.pick_laggards(
                weekly_returns,
                industry_map,
                trade_date,
                industries,
                self.config.laggard_pct,
            )
            if laggards.empty:
                continue
            tickers = laggards["ts_code"].unique().tolist()
            industries_snapshot = sorted(laggards["industry_name"].unique().tolist())
            entry_date = self._next_trading_day(trading_calendar, trade_date)
            if entry_date is None:
                continue
            exit_date = self._forward_trading_day(
                trading_calendar, entry_date, self.config.hold_days
            )
            path, stock_returns = self._evaluate_portfolio(
                daily_returns, tickers, entry_date, exit_date
            )
            if path.empty:
                continue
            total_return = float((1 + path).prod() - 1)
            events.append(
                EventResult(
                    trade_date=trade_date,
                    entry_date=entry_date,
                    exit_date=exit_date,
                    tickers=tickers,
                    total_return=total_return,
                    path=path,
                    industries=industries_snapshot,
                    stock_returns=stock_returns,
                    laggard_snapshot=laggards[
                        ["ts_code", "industry_name", "weekly_ret"]
                    ]
                    .to_dict("records"),
                )
            )
        return events

    def _next_trading_day(
        self, trading_calendar: pd.Series, trade_date: pd.Timestamp
    ) -> pd.Timestamp | None:
        idx = trading_calendar.searchsorted(trade_date)
        if idx >= len(trading_calendar) - 1:
            return None
        return trading_calendar[idx + 1]

    def _forward_trading_day(
        self, trading_calendar: pd.Series, entry_date: pd.Timestamp, hold_days: int
    ) -> pd.Timestamp:
        idx = trading_calendar.searchsorted(entry_date)
        target = min(len(trading_calendar) - 1, idx + hold_days)
        return trading_calendar[target]

    def _evaluate_portfolio(
        self,
        daily_returns: pd.DataFrame,
        tickers: List[str],
        entry_date: pd.Timestamp,
        exit_date: pd.Timestamp,
    ) -> Tuple[pd.Series, Dict[str, float]]:
        mask = (
            daily_returns["ts_code"].isin(tickers)
            & (daily_returns["trade_date"] >= entry_date)
            & (daily_returns["trade_date"] <= exit_date)
        )
        subset = daily_returns.loc[mask, ["trade_date", "ts_code", "ret"]]
        if subset.empty:
            return pd.Series(dtype=float), {}
        grouped = subset.groupby(["trade_date"])["ret"].mean()
        grouped.sort_index(inplace=True)
        stock_returns = subset.groupby("ts_code")["ret"].apply(lambda x: (1 + x).prod() - 1)
        return grouped, stock_returns.to_dict()
