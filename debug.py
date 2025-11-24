import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent / "src"))

from backtest_industry.config import StrategyConfig
from backtest_industry.data import MockDataProvider
from backtest_industry.engine import BacktestEngine
from backtest_industry import concentration
from tests.test_engine import _build_mock_daily, _build_industry_map

provider = MockDataProvider(_build_mock_daily(), _build_industry_map())
config = StrategyConfig(start_date="2020-01-01", end_date="2020-03-31", trigger_threshold=-1.0, laggard_pct=0.5, hold_days=5, top_industry_n=1)
engine = BacktestEngine(provider, config)
daily_bars = provider.get_daily_bars(config.start_date, config.end_date)
print("daily bars", daily_bars.head())
daily = concentration.build_daily_returns(daily_bars)
print("daily returns", daily.head())
weekly = concentration.resample_weekly_returns(daily, config.weekly_resample_rule)
market = concentration.compute_market_concentration(weekly, config.market_top_pct)
print(market.head())
industry_map = provider.get_industry_mapping()
industry_conc = concentration.compute_industry_concentration(weekly, industry_map, config.industry_top_pct)
print(industry_conc.head())
summary = engine.run()
print("events", summary.event_count)
