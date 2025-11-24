"""TuShare-facing data access layer with simple on-disk caching."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import logging

import pandas as pd

from .config import StrategyConfig

try:  # Optional to keep tests light-weight without an actual TuShare install.
    import tushare as ts
except ImportError:  # pragma: no cover - exercised only when tushare is missing at runtime.
    ts = None  # type: ignore

_LOGGER = logging.getLogger(__name__)


class DataProviderError(RuntimeError):
    """Raised when a data fetch failure occurs."""


class BaseDataProvider:
    """Interface expected by the backtest engine."""

    def get_daily_bars(self, start_date: str, end_date: str) -> pd.DataFrame:
        raise NotImplementedError

    def get_industry_mapping(self, level: int = 2) -> pd.DataFrame:
        raise NotImplementedError


@dataclass(slots=True)
class TuShareDataProvider(BaseDataProvider):
    """Thin wrapper around tushare pro_api with minimal caching."""

    config: StrategyConfig

    def __post_init__(self) -> None:
        if ts is None:
            raise ImportError("tushare is required but not installed")
        ts.set_token(self.config.tushare_token)
        self._pro = ts.pro_api()
        self.cache_dir: Path = self.config.ensure_cache_dir()

    # --------------------- Public API ---------------------
    def get_daily_bars(self, start_date: str, end_date: str) -> pd.DataFrame:
        cache_file = self.cache_dir / f"daily_{start_date}_{end_date}.parquet"
        if cache_file.exists():
            return pd.read_parquet(cache_file)

        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        fields = "ts_code,trade_date,close,pre_close,adj_factor"
        frames: list[pd.DataFrame] = []
        for chunk_start, chunk_end in self._iter_daily_chunks(start_dt, end_dt):
            chunk = self._fetch_daily_chunk(chunk_start, chunk_end, fields)
            if chunk.empty:
                _LOGGER.warning("Empty daily chunk %s -> %s", chunk_start.date(), chunk_end.date())
                continue
            frames.append(chunk)

        if not frames:
            raise DataProviderError("No daily bars fetched; verify token and quota")

        df = pd.concat(frames, ignore_index=True)
        df.sort_values(["ts_code", "trade_date"], inplace=True)
        df.drop_duplicates(subset=["ts_code", "trade_date"], keep="last", inplace=True)
        df.to_parquet(cache_file, index=False)
        return df

    def get_industry_mapping(self, level: int = 2) -> pd.DataFrame:  # noqa: D401
        cache_file = self.cache_dir / f"sw_level{level}_mapping.parquet"
        if cache_file.exists():
            return pd.read_parquet(cache_file)

        try:
            sw_meta = self._pro.index_classify(level=f"L{level}", src=self.config.sw_src)
        except Exception as exc:  # pragma: no cover
            raise DataProviderError(f"Failed to load ShenWan L{level} list: {exc}") from exc

        code_fields = ["index_code", f"l{level}_code", "l2_code", "l1_code", "sw_index_code"]
        members: list[pd.DataFrame] = []
        for _, row in sw_meta.iterrows():
            index_code = None
            for field in code_fields:
                value = row.get(field)
                if value:
                    index_code = value
                    break
            if not index_code:
                continue
            try:
                chunk = self._pro.index_member_all(l2_code=index_code)
            except Exception as exc:  # pragma: no cover
                _LOGGER.warning("index_member_all failed for %s: %s", index_code, exc)
                continue
            if chunk.empty:
                continue
            if "is_new" in chunk.columns:
                chunk = chunk[chunk["is_new"].fillna("Y") == self.config.sw_is_new]
            if chunk.empty:
                continue
            if "ts_code" not in chunk.columns and "con_code" in chunk.columns:
                chunk = chunk.rename(columns={"con_code": "ts_code"})
            chunk = chunk.assign(industry_name=row.get("industry_name", ""))
            members.append(chunk)

        if not members:
            raise DataProviderError("No ShenWan constituents fetched; check TuShare quota or params")

        mapping = pd.concat(members, ignore_index=True)[["ts_code", "industry_name", "in_date", "out_date"]]
        mapping.drop_duplicates(subset=["ts_code", "industry_name", "in_date"], inplace=True)
        mapping.to_parquet(cache_file, index=False)
        return mapping

    # --------------------- Helpers ---------------------
    def _iter_daily_chunks(self, start_dt: pd.Timestamp, end_dt: pd.Timestamp):
        chunk_months = max(1, self.config.daily_chunk_months)
        offset = pd.DateOffset(months=chunk_months)
        current = start_dt
        one_day = pd.Timedelta(days=1)
        while current <= end_dt:
            tentative_end = current + offset - one_day
            chunk_end = tentative_end if tentative_end <= end_dt else end_dt
            yield current, chunk_end
            current = chunk_end + one_day

    def _fetch_daily_chunk(
        self,
        chunk_start: pd.Timestamp,
        chunk_end: pd.Timestamp,
        fields: str,
    ) -> pd.DataFrame:
        bulk = self._download_daily_range(chunk_start, chunk_end, fields)
        if not self._needs_trade_date_split(bulk, chunk_start, chunk_end):
            return bulk
        _LOGGER.info(
            "Chunk %s -> %s exceeds row limit or coverage gaps detected, switching to per-trade-date fetch",
            chunk_start.date(),
            chunk_end.date(),
        )
        return self._fetch_daily_by_trade_date(chunk_start, chunk_end, fields)

    def _download_daily_range(
        self,
        chunk_start: pd.Timestamp,
        chunk_end: pd.Timestamp,
        fields: str,
    ) -> pd.DataFrame:
        start_str = chunk_start.strftime("%Y%m%d")
        end_str = chunk_end.strftime("%Y%m%d")
        _LOGGER.info("Fetching daily chunk %s -> %s", start_str, end_str)
        if chunk_start == chunk_end:
            daily = self._pro.daily(trade_date=start_str, fields=fields)
            adj = self._pro.adj_factor(trade_date=start_str, fields="ts_code,trade_date,adj_factor")
        else:
            daily = self._pro.daily(start_date=start_str, end_date=end_str, fields=fields)
            adj = self._pro.adj_factor(start_date=start_str, end_date=end_str, fields="ts_code,trade_date,adj_factor")
        if daily.empty:
            return pd.DataFrame(columns=fields.split(","))

        if adj.empty:
            adj = pd.DataFrame(columns=["ts_code", "trade_date", "adj_factor"])
        merged = daily.merge(adj, on=["ts_code", "trade_date"], how="left", suffixes=("", "_adj"))
        if "adj_factor_adj" in merged.columns:
            merged["adj_factor"] = merged["adj_factor"].fillna(merged.pop("adj_factor_adj"))
        else:
            merged["adj_factor"] = merged["adj_factor"].fillna(1.0)
        return merged

    def _fetch_daily_by_trade_date(
        self,
        chunk_start: pd.Timestamp,
        chunk_end: pd.Timestamp,
        fields: str,
    ) -> pd.DataFrame:
        trade_dates = self._get_trade_dates(chunk_start, chunk_end)
        if not trade_dates:
            return pd.DataFrame(columns=fields.split(","))
        frames: list[pd.DataFrame] = []
        for trade_date in trade_dates:
            frames.append(self._download_daily_range(trade_date, trade_date, fields))
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=fields.split(","))

    def _needs_trade_date_split(
        self,
        frame: pd.DataFrame,
        chunk_start: pd.Timestamp,
        chunk_end: pd.Timestamp,
    ) -> bool:
        if chunk_start >= chunk_end:
            return False
        if frame.empty:
            trade_dates = self._get_trade_dates(chunk_start, chunk_end)
            return bool(trade_dates)
        if len(frame) >= self.config.daily_row_limit:
            return True
        trade_dates = pd.to_datetime(frame["trade_date"])
        gap_start = (trade_dates.min() - chunk_start).days
        gap_end = (chunk_end - trade_dates.max()).days
        return gap_start > 5 or gap_end > 5

    def _get_trade_dates(self, chunk_start: pd.Timestamp, chunk_end: pd.Timestamp) -> list[pd.Timestamp]:
        start_str = chunk_start.strftime("%Y%m%d")
        end_str = chunk_end.strftime("%Y%m%d")
        cal = self._pro.trade_cal(
            exchange=self.config.calendar,
            start_date=start_str,
            end_date=end_str,
            fields="cal_date,is_open",
        )
        if cal.empty:
            return []
        open_days = cal[cal["is_open"] == 1]["cal_date"].astype(str)
        return [pd.to_datetime(day) for day in open_days]


class MockDataProvider(BaseDataProvider):
    """Convenience provider for unit tests based on user-supplied frames."""

    def __init__(self, daily_bars: pd.DataFrame, industry_map: pd.DataFrame):
        self._daily = daily_bars.copy()
        self._daily["trade_date"] = pd.to_datetime(self._daily["trade_date"])
        self._industry = industry_map.copy()

    def get_daily_bars(self, start_date: str, end_date: str) -> pd.DataFrame:  # noqa: D401
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        mask = (self._daily.trade_date >= start) & (self._daily.trade_date <= end)
        return self._daily.loc[mask].copy()

    def get_industry_mapping(self, level: int = 2) -> pd.DataFrame:  # noqa: D401
        return self._industry.copy()
