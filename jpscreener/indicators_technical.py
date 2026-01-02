from typing import Dict, List

import numpy as np
import pandas as pd

from .rules_technical import evaluate_technical


def moving_averages(df: pd.DataFrame, windows: List[int]) -> Dict[int, float]:
    ma: Dict[int, float] = {}
    if df is None or df.empty:
        return ma
    closes = df["Close"]
    for w in windows:
        series = closes.rolling(window=w).mean()
        ma[w] = float(series.iloc[-1]) if len(series) >= w and not np.isnan(series.iloc[-1]) else np.nan
    return ma


def latest_close(df: pd.DataFrame) -> float:
    if df is None or df.empty:
        return np.nan
    try:
        return float(df["Close"].iloc[-1])
    except Exception:
        return np.nan


def recent_high_low(df: pd.DataFrame, window: int = 20) -> Dict[str, float]:
    if df is None or df.empty:
        return {"high": np.nan, "low": np.nan}
    tail = df.tail(window)
    return {
        "high": float(tail["High"].max()) if not tail.empty else np.nan,
        "low": float(tail["Low"].min()) if not tail.empty else np.nan,
    }


def previous_high(df: pd.DataFrame, window: int = 20) -> float:
    """Highest high over the previous `window` periods, excluding the latest row."""
    if df is None or df.empty or len(df) <= window:
        return np.nan
    window_slice = df["High"].iloc[-(window + 1) : -1]
    if window_slice.empty:
        return np.nan
    return float(window_slice.max())


def build_technical_record(ticker: str, df: pd.DataFrame) -> Dict[str, object]:
    ma = moving_averages(df, [20, 50, 200])
    close = latest_close(df)
    hl = recent_high_low(df, 20)
    prev_high = previous_high(df, 20)
    metrics = {
        "close": close,
        "ma20": ma.get(20, np.nan),
        "ma50": ma.get(50, np.nan),
        "ma200": ma.get(200, np.nan),
        "high20": hl["high"],
        "low20": hl["low"],
        "prev20_high": prev_high,
    }
    rule = evaluate_technical(metrics)
    return {
        "ticker": ticker,
        "metrics": metrics,
        "passed_rules": rule["passed"],
        "failed_rules": rule["failed"],
        "missing_rules": rule["missing"],
        "entry_ok": rule["entry_ok"],
        "regime_ok": rule["regime_ok"],
        "setup_ok": rule["setup_ok"],
    }
