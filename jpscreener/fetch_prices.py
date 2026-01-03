from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List
import tempfile

import numpy as np
import pandas as pd
import yfinance as yf
from yfinance import utils as yutils


def configure_yfinance_cache() -> None:
    """Point yfinance tz cache to a temp directory to avoid permissions/race issues on runners."""
    cache_dir = Path(tempfile.gettempdir()) / "py-yfinance-tzcache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    try:
        yutils.set_tz_cache_location(str(cache_dir))
    except Exception:
        # cache failures should not block downloads
        pass


@dataclass
class PriceStats:
    ticker: str
    adv_jpy: float | None
    adtv_shares: float | None
    history_points: int
    errors: List[str]


def compute_adv(df: pd.DataFrame, window: int) -> tuple[float | None, float | None, int]:
    if df is None or df.empty:
        return None, None, 0
    sliced = df.tail(window)
    if sliced.empty:
        return None, None, len(df)
    money = sliced["Close"] * sliced["Volume"]
    adv_jpy = float(np.nanmean(money.values)) if not money.empty else None
    adtv = float(np.nanmean(sliced["Volume"].values)) if not sliced["Volume"].empty else None
    return adv_jpy, adtv, len(df)


def fetch_price_windows(tickers: List[str], adv_window: int, adv_period: str, logger, timeout: float = 10.0) -> Dict[str, PriceStats]:
    """
    Download daily prices for tickers and compute ADV/ADT shares.
    Uses yfinance bulk download to reduce round-trips.
    """
    configure_yfinance_cache()
    if not tickers:
        return {}

    try:
        data = yf.download(
            tickers=tickers,
            period=adv_period,
            interval="1d",
            auto_adjust=False,
            progress=False,
            threads=True,
        )
    except Exception as exc:  # pragma: no cover - yfinance defensive
        logger.error("price download failed: %s", exc)
        data = pd.DataFrame()

    results: Dict[str, PriceStats] = {}
    if isinstance(data, pd.DataFrame) and not data.empty and isinstance(data.columns, pd.MultiIndex):
        for ticker in tickers:
            errors: List[str] = []
            try:
                close = data["Close"][ticker]
                vol = data["Volume"][ticker]
                df = pd.DataFrame({"Close": close, "Volume": vol}).dropna(how="all")
            except Exception as exc:
                errors = [f"slice error: {exc}"]
                df = pd.DataFrame()
            adv_jpy, adtv, count = compute_adv(df, adv_window)
            results[ticker] = PriceStats(ticker, adv_jpy, adtv, count, errors)
    else:
        # Fallback: one-by-one to salvage partial data
        for ticker in tickers:
            errors: List[str] = []
            try:
                df = yf.download(
                    tickers=ticker,
                    period=adv_period,
                    interval="1d",
                    auto_adjust=False,
                    progress=False,
                )
            except Exception as exc:  # pragma: no cover - yfinance defensive
                errors.append(f"download error: {exc}")
                df = pd.DataFrame()
            adv_jpy, adtv, count = compute_adv(df, adv_window)
            results[ticker] = PriceStats(ticker, adv_jpy, adtv, count, errors)
    return results


def fetch_recent_prices(tickers: List[str], period: str, logger) -> Dict[str, pd.DataFrame]:
    """Return per-ticker price DataFrames (Close, High, Low, Volume)."""
    configure_yfinance_cache()
    if not tickers:
        return {}
    try:
        data = yf.download(
            tickers=tickers,
            period=period,
            interval="1d",
            auto_adjust=False,
            progress=False,
            threads=True,
        )
    except Exception as exc:  # pragma: no cover
        logger.error("price download failed: %s", exc)
        data = pd.DataFrame()

    result: Dict[str, pd.DataFrame] = {}
    if isinstance(data, pd.DataFrame) and not data.empty and isinstance(data.columns, pd.MultiIndex):
        for ticker in tickers:
            try:
                close = data["Close"][ticker]
                high = data["High"][ticker]
                low = data["Low"][ticker]
                vol = data["Volume"][ticker]
                df = pd.DataFrame({"Close": close, "High": high, "Low": low, "Volume": vol}).dropna(how="all")
            except Exception:
                df = pd.DataFrame()
            result[ticker] = df
    else:
        for ticker in tickers:
            try:
                df = yf.download(
                    tickers=ticker,
                    period=period,
                    interval="1d",
                    auto_adjust=False,
                    progress=False,
                )
                df = df[["Close", "High", "Low", "Volume"]] if not df.empty else pd.DataFrame()
            except Exception:
                df = pd.DataFrame()
            result[ticker] = df
    return result
