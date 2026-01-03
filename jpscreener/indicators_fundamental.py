from typing import Dict, Iterable, Sequence

import numpy as np
import pandas as pd

INCOME_ROWS = {
    "revenue": ["Total Revenue", "TotalRevenue"],
    "operating_income": ["Operating Income", "OperatingIncome"],
    "net_income": ["Net Income", "NetIncome"],
}

BALANCE_ROWS = {
    "equity": [
        "Total Stockholder Equity",
        "Stockholders Equity",
        "Total Equity Gross Minority Interest",
    ],
    "total_assets": ["Total Assets", "TotalAssets"],
    "total_liabilities": ["Total Liab", "TotalLiabilitiesNetMinorityInterest", "Total Liabilities"],
}

CASHFLOW_ROWS = {
    "cfo": ["Total Cash From Operating Activities", "Operating Cash Flow"],
    "capex": ["Capital Expenditures", "Capital Expenditure"],
}


def _latest_column(columns: Sequence) -> str:
    if len(columns) == 0:
        raise ValueError("No columns available to select latest.")

    try:
        parsed = pd.to_datetime(list(columns), errors="coerce")
        if parsed.notna().any():
            idx = int(parsed.fillna(pd.Timestamp.min).argmax())
            return list(columns)[idx]
    except Exception:
        pass

    try:
        return sorted(columns, key=lambda c: str(c), reverse=True)[0]
    except Exception:
        return list(columns)[0]


def latest_series(df: pd.DataFrame) -> pd.Series:
    if df is None or df.empty:
        return pd.Series(dtype="float64")
    col = _latest_column(df.columns)
    return df[col]


def pick_value(series: pd.Series, candidates: Iterable[str]) -> float:
    index_map = {str(idx).lower(): idx for idx in series.index}
    for name in candidates:
        key = name.lower()
        if key in index_map:
            try:
                return float(series[index_map[key]])
            except Exception:
                return np.nan
    return np.nan


def safe_div(numerator: float, denominator: float) -> float:
    if np.isnan(numerator) or np.isnan(denominator) or denominator == 0:
        return np.nan
    try:
        return float(numerator) / float(denominator)
    except Exception:
        return np.nan


def extract_metrics(statements: Dict[str, pd.DataFrame], info: Dict) -> Dict[str, float]:
    income_series = latest_series(statements.get("income", pd.DataFrame()))
    balance_series = latest_series(statements.get("balance", pd.DataFrame()))
    cash_series = latest_series(statements.get("cashflow", pd.DataFrame()))

    revenue = pick_value(income_series, INCOME_ROWS["revenue"])
    op_income = pick_value(income_series, INCOME_ROWS["operating_income"])
    net_income = pick_value(income_series, INCOME_ROWS["net_income"])

    equity = pick_value(balance_series, BALANCE_ROWS["equity"])
    assets = pick_value(balance_series, BALANCE_ROWS["total_assets"])
    liabilities = pick_value(balance_series, BALANCE_ROWS["total_liabilities"])

    cfo = pick_value(cash_series, CASHFLOW_ROWS["cfo"])
    capex = pick_value(cash_series, CASHFLOW_ROWS["capex"])
    fcf = cfo - capex if not np.isnan(cfo) and not np.isnan(capex) else np.nan

    price = _get_info_value(info, ["currentPrice", "regularMarketPrice", "previousClose"])
    shares = _get_info_value(info, ["sharesOutstanding"])

    per = _get_info_value(info, ["trailingPE"])
    if np.isnan(per) and not np.isnan(price) and not np.isnan(net_income) and not np.isnan(shares) and shares != 0:
        per = safe_div(price, net_income / shares)

    pbr = _get_info_value(info, ["priceToBook"])
    if np.isnan(pbr) and not np.isnan(price) and not np.isnan(equity) and not np.isnan(shares) and shares != 0:
        pbr = safe_div(price, equity / shares)

    return {
        "roe": safe_div(net_income, equity),
        "op_margin": safe_div(op_income, revenue),
        "equity_ratio": safe_div(equity, assets),
        "de_ratio": safe_div(liabilities, equity),
        "cfo": cfo,
        "fcf": fcf,
        "per": per,
        "pbr": pbr,
        "dividend_yield": _get_info_value(info, ["dividendYield"]),
        "price": price,
        "shares_outstanding": shares,
    }


def _get_info_value(info: Dict, keys: Sequence[str]) -> float:
    for key in keys:
        if key in info and info[key] is not None:
            try:
                return float(info[key])
            except Exception:
                continue
    return np.nan
