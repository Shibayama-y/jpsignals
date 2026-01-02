import argparse
import json
import logging
from pathlib import Path
import sys
from typing import Dict, Iterable, List, Sequence

import numpy as np
import pandas as pd
import yfinance as yf

ADV_PERIOD = "3mo"
ADV_WINDOW = 20
DEFAULT_TICKER_FILE = Path(__file__).resolve().parent.parent / "data" / "universe.txt"

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

RULE_DEFS = [
    ("ROE", "roe", lambda v: v >= 0.08),
    ("OpMargin", "op_margin", lambda v: v >= 0.05),
    ("EquityRatio", "equity_ratio", lambda v: v >= 0.30),
    ("CFO", "cfo", lambda v: v > 0),
    ("PER", "per", lambda v: v <= 20),
    ("PBR", "pbr", lambda v: v <= 1.5),
]

KEY_METRICS_FOR_STRICT = ["roe", "op_margin", "equity_ratio", "cfo", "per", "pbr"]
DEFAULT_MIN_SCORE = 60


def normalize_ticker(ticker: str) -> str:
    """Append .T if a bare 4-digit code is supplied."""

    stripped = ticker.strip()
    if stripped.isdigit() and len(stripped) == 4:
        return f"{stripped}.T"
    return stripped


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


def _get_info_value(info: Dict, keys: Sequence[str]) -> float:
    for key in keys:
        if key in info and info[key] is not None:
            try:
                return float(info[key])
            except Exception:
                continue
    return np.nan


def load_tickers_from_file(path: Path, logger: logging.Logger) -> List[str]:
    """
    Load tickers from CSV-like file with column "Ticker" (default format in data/universe.txt).
    Falls back to cp932 if UTF-8 decode fails.
    """

    if not path.exists():
        raise FileNotFoundError(f"tickers file not found: {path}")
    try:
        df = pd.read_csv(path)
    except UnicodeDecodeError:
        df = pd.read_csv(path, encoding="cp932")
    except Exception as exc:
        raise ValueError(f"failed to read tickers file {path}: {exc}") from exc

    ticker_col = None
    for candidate in ["Ticker", "ticker", "Symbol", "symbol"]:
        if candidate in df.columns:
            ticker_col = candidate
            break
    if ticker_col is None:
        raise ValueError(f"Ticker column not found in {path}")

    tickers = [str(v).strip() for v in df[ticker_col].tolist() if str(v).strip()]
    logger.info("Loaded %d tickers from %s", len(tickers), path)
    return tickers


def compute_adv_jpy(df: pd.DataFrame, window: int = ADV_WINDOW) -> float:
    """Mean of Close*Volume over the last `window` rows."""

    if df is None or df.empty or "Close" not in df or "Volume" not in df:
        return np.nan
    tail = df.tail(window)
    if tail.empty:
        return np.nan
    try:
        return float(np.nanmean((tail["Close"] * tail["Volume"]).values))
    except Exception:
        return np.nan


def fetch_adv_bulk(tickers: List[str], logger: logging.Logger) -> Dict[str, Dict[str, object]]:
    """Download recent prices in bulk and compute ADV_JPY_20 for each ticker."""

    if not tickers:
        return {}
    try:
        data = yf.download(
            tickers=tickers,
            period=ADV_PERIOD,
            interval="1d",
            auto_adjust=True,
            progress=False,
            threads=True,
        )
    except Exception as exc:  # pragma: no cover - yfinance defensive
        logger.error("price download failed: %s", exc)
        data = pd.DataFrame()

    results: Dict[str, Dict[str, object]] = {t: {"adv_jpy": np.nan, "errors": []} for t in tickers}
    if isinstance(data, pd.DataFrame) and not data.empty and isinstance(data.columns, pd.MultiIndex):
        for ticker in tickers:
            errors: List[str] = []
            try:
                close = data["Close"][ticker]
                vol = data["Volume"][ticker]
                df = pd.DataFrame({"Close": close, "Volume": vol}).dropna(how="all")
            except Exception as exc:
                errors.append(f"slice error: {exc}")
                df = pd.DataFrame()
            results[ticker] = {"adv_jpy": compute_adv_jpy(df), "errors": errors}
    else:
        # Fallback: per-ticker download to salvage partial results
        for ticker in tickers:
            errors: List[str] = []
            try:
                df = yf.download(
                    tickers=ticker,
                    period=ADV_PERIOD,
                    interval="1d",
                    auto_adjust=True,
                    progress=False,
                )
            except Exception as exc:  # pragma: no cover - yfinance defensive
                errors.append(f"download error: {exc}")
                df = pd.DataFrame()
            results[ticker] = {"adv_jpy": compute_adv_jpy(df), "errors": errors}
    return results


def load_statement(tkr: yf.Ticker, attr_candidates: Sequence[str], errors: List[str]) -> pd.DataFrame:
    for attr in attr_candidates:
        try:
            obj = getattr(tkr, attr, None)
        except Exception as exc:  # pragma: no cover - defensive for yfinance internals
            errors.append(f"{attr} access error: {exc}")
            continue

        df = None
        if callable(obj):
            try:
                df = obj()
            except Exception as exc:  # pragma: no cover - defensive
                errors.append(f"{attr} call error: {exc}")
                continue
        else:
            df = obj

        if isinstance(df, pd.DataFrame) and not df.empty:
            return df
    return pd.DataFrame()


def collect_statements(tkr: yf.Ticker, errors: List[str]) -> Dict[str, pd.DataFrame]:
    income = load_statement(tkr, ["ttm_income_stmt", "income_stmt", "financials"], errors)
    balance = load_statement(tkr, ["balance_sheet"], errors)
    cashflow = load_statement(tkr, ["ttm_cashflow", "cashflow"], errors)
    return {"income": income, "balance": balance, "cashflow": cashflow}


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


def score_metrics(metrics: Dict[str, float]) -> int:
    score = 0.0

    roe = metrics.get("roe", np.nan)
    if not np.isnan(roe):
        if roe >= 0.10:
            score += 25
        elif roe >= 0.08:
            score += 15

    op_margin = metrics.get("op_margin", np.nan)
    if not np.isnan(op_margin):
        if op_margin >= 0.10:
            score += 15
        elif op_margin >= 0.05:
            score += 10
        elif op_margin > 0:
            score += 5

    equity_ratio = metrics.get("equity_ratio", np.nan)
    if not np.isnan(equity_ratio):
        if equity_ratio >= 0.50:
            score += 15
        elif equity_ratio >= 0.30:
            score += 10

    de_ratio = metrics.get("de_ratio", np.nan)
    if not np.isnan(de_ratio) and de_ratio > 5:
        score -= 5

    cfo = metrics.get("cfo", np.nan)
    if not np.isnan(cfo) and cfo > 0:
        score += 10

    fcf = metrics.get("fcf", np.nan)
    if not np.isnan(fcf) and fcf > 0:
        score += 10

    pbr = metrics.get("pbr", np.nan)
    if not np.isnan(pbr):
        if pbr <= 1.0:
            score += 10
        elif pbr <= 1.5:
            score += 5

    per = metrics.get("per", np.nan)
    if not np.isnan(per):
        if per <= 15:
            score += 5
        elif per <= 20:
            score += 3

    score = max(0.0, score)
    return int(min(100, round(score)))


def evaluate_rules(metrics: Dict[str, float], strict: bool) -> Dict[str, object]:
    passed: List[str] = []
    failed: List[str] = []
    missing: List[str] = []

    for name, key, predicate in RULE_DEFS:
        value = metrics.get(key, np.nan)
        if np.isnan(value):
            missing.append(name)
            continue
        if predicate(value):
            passed.append(name)
        else:
            failed.append(name)

    strict_missing = sum(1 for key in KEY_METRICS_FOR_STRICT if np.isnan(metrics.get(key, np.nan)))
    strict_blocked = strict and strict_missing >= 3
    hard_pass = len(passed) > len(failed) and (len(passed) + len(failed)) > 0 and not strict_blocked

    return {
        "passed": passed,
        "failed": failed,
        "missing": missing,
        "hard_pass": hard_pass,
        "strict_blocked": strict_blocked,
        "strict_missing": strict_missing,
    }


def _json_safe(value: float):
    if isinstance(value, (float, np.floating)) and np.isnan(value):
        return None
    return value


def format_metrics_summary(metrics: Dict[str, float]) -> str:
    def fmt_ratio(val: float) -> str:
        if np.isnan(val):
            return "NaN"
        return f"{val:.2f}"

    def fmt_sign(val: float) -> str:
        if np.isnan(val):
            return "NaN"
        if val > 0:
            return "+"
        if val < 0:
            return "-"
        return "0"

    return " ".join(
        [
            f"ROE={fmt_ratio(metrics.get('roe', np.nan))}",
            f"OpM={fmt_ratio(metrics.get('op_margin', np.nan))}",
            f"EqR={fmt_ratio(metrics.get('equity_ratio', np.nan))}",
            f"PER={fmt_ratio(metrics.get('per', np.nan))}",
            f"PBR={fmt_ratio(metrics.get('pbr', np.nan))}",
            f"CFO={fmt_sign(metrics.get('cfo', np.nan))}",
            f"FCF={fmt_sign(metrics.get('fcf', np.nan))}",
        ]
    )


def analyze_ticker(
    ticker: str,
    timeout: float,
    min_score: int,
    strict: bool,
    logger: logging.Logger,
    adv_jpy: float | None = np.nan,
    adv_errors: Sequence[str] | None = None,
) -> Dict[str, object]:
    normalized = normalize_ticker(ticker)
    errors: List[str] = []
    if adv_errors:
        errors.extend(adv_errors)

    tkr = yf.Ticker(normalized)

    try:
        statements = collect_statements(tkr, errors)
    except Exception as exc:  # pragma: no cover - yfinance defensive
        errors.append(f"statement fetch error: {exc}")
        statements = {"income": pd.DataFrame(), "balance": pd.DataFrame(), "cashflow": pd.DataFrame()}

    try:
        info = tkr.info or {}
    except Exception as exc:  # pragma: no cover - yfinance defensive
        errors.append(f"info fetch error: {exc}")
        info = {}

    metrics = extract_metrics(statements, info)
    score = score_metrics(metrics)
    rule_eval = evaluate_rules(metrics, strict=strict)
    overall_pass = score >= min_score and rule_eval["hard_pass"]

    if strict and rule_eval["strict_blocked"]:
        logger.debug("%s failed strict mode due to %d missing key metrics", normalized, rule_eval["strict_missing"])

    record = {
        "ticker": normalized,
        "score": score,
        "pass": overall_pass,
        "adv_jpy": adv_jpy,
        "metrics": metrics,
        "passed_rules": rule_eval["passed"],
        "failed_rules": rule_eval["failed"],
        "missing_rules": rule_eval["missing"],
        "errors": errors,
    }
    return record


def format_text_record(record: Dict[str, object]) -> str:
    status = "PASS" if record.get("pass") else "FAIL"
    score = record.get("score")
    ticker = record.get("ticker")
    metrics = record.get("metrics", {})

    summary = format_metrics_summary(metrics) if isinstance(metrics, dict) else ""
    adv_jpy = record.get("adv_jpy")
    passed_rules = ",".join(record.get("passed_rules", []))
    failed_rules = ",".join(record.get("failed_rules", []))
    missing_rules = ",".join(record.get("missing_rules", []))
    errors = ",".join(record.get("errors", []))

    parts = [
        f"{ticker}",
        f"score={score}",
        status,
        summary,
        (f"ADV_JPY_20={adv_jpy:,.0f}" if isinstance(adv_jpy, (int, float)) and not np.isnan(adv_jpy) else ""),
        f"pass=[{passed_rules}]",
        f"fail=[{failed_rules}]",
        f"missing=[{missing_rules}]",
    ]
    if errors:
        parts.append(f"errors=[{errors}]")
    return "  ".join(part for part in parts if part)


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Japanese stock fundamental screener using free yfinance data.",
    )
    parser.add_argument(
        "tickers",
        nargs="*",
        help="Optional tickers (e.g., 7203.T 9984.T). If omitted, load from --tickers-file.",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (text or json lines)",
    )
    parser.add_argument(
        "--min-score",
        type=int,
        default=DEFAULT_MIN_SCORE,
        help="Minimum score to pass (0-100).",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail if 3 or more key metrics are missing (ROE, OpMargin, EquityRatio, CFO, PER, PBR).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=10.0,
        help="(yfinance default session is used; kept for compatibility, currently unused)",
    )
    parser.add_argument(
        "--log-level",
        choices=["ERROR", "WARNING", "INFO", "DEBUG"],
        default="INFO",
        help="Logging level.",
    )
    parser.add_argument(
        "--min-adv-jpy",
        type=float,
        default=0.0,
        help="Filter out tickers whose ADV_JPY_20 (mean Close*Volume over last 20 trading days) is below this value.",
    )
    parser.add_argument(
        "--tickers-file",
        default=str(DEFAULT_TICKER_FILE),
        help="Path to CSV file with column 'Ticker' (default: data/universe.txt).",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s:%(message)s")
    logger = logging.getLogger("jf_screener")

    tickers_input: List[str] = []
    if args.tickers:
        tickers_input = list(args.tickers)
    else:
        try:
            tickers_input = load_tickers_from_file(Path(args.tickers_file), logger)
        except Exception as exc:
            logger.error("Failed to load tickers from file: %s", exc)
            return 1
    if not tickers_input:
        logger.error("No tickers provided (neither CLI nor file).")
        return 1

    normalized_tickers = [normalize_ticker(t) for t in tickers_input]
    logger.info("Starting screening for %d tickers", len(normalized_tickers))
    adv_stats = fetch_adv_bulk(normalized_tickers, logger)

    results = []
    eligible: List[str] = []
    for t in normalized_tickers:
        adv_info = adv_stats.get(t, {"adv_jpy": np.nan, "errors": ["adv_jpy missing"]})
        adv_jpy = adv_info.get("adv_jpy", np.nan)
        adv_errors = adv_info.get("errors", [])
        if args.min_adv_jpy > 0:
            if np.isnan(adv_jpy):
                logger.info("%s skipped: ADV_JPY_20 unavailable (requires >= %.0f)", t, args.min_adv_jpy)
                results.append(
                    {
                        "ticker": t,
                        "score": 0,
                        "pass": False,
                        "adv_jpy": adv_jpy,
                        "metrics": {},
                        "passed_rules": [],
                        "failed_rules": [],
                        "missing_rules": [],
                        "errors": list(adv_errors) + [f"ADV_JPY_20 unavailable (< {args.min_adv_jpy})"],
                    }
                )
                continue
            if adv_jpy < args.min_adv_jpy:
                logger.info("%s skipped: ADV_JPY_20=%.0f < %.0f", t, adv_jpy, args.min_adv_jpy)
                results.append(
                    {
                        "ticker": t,
                        "score": 0,
                        "pass": False,
                        "adv_jpy": adv_jpy,
                        "metrics": {},
                        "passed_rules": [],
                        "failed_rules": [],
                        "missing_rules": [],
                        "errors": list(adv_errors) + [f"ADV_JPY_20={adv_jpy:.0f} below min_adv_jpy={args.min_adv_jpy:.0f}"],
                    }
                )
                continue
        eligible.append(t)

    for t in eligible:
        try:
            adv_info = adv_stats.get(t, {"adv_jpy": np.nan, "errors": []})
            record = analyze_ticker(
                t,
                timeout=args.timeout,
                min_score=args.min_score,
                strict=args.strict,
                logger=logger,
                adv_jpy=adv_info.get("adv_jpy", np.nan),
                adv_errors=adv_info.get("errors", []),
            )
            results.append(record)
            logger.info(
                "%s -> score=%s pass=%s errors=%d",
                record.get("ticker"),
                record.get("score"),
                record.get("pass"),
                len(record.get("errors", [])),
            )
        except Exception as exc:  # pragma: no cover - defensive
            logger.exception("Unhandled error for %s", t)
            results.append(
                {
                    "ticker": normalize_ticker(t),
                    "score": 0,
                    "pass": False,
                    "adv_jpy": np.nan,
                    "metrics": {},
                    "passed_rules": [],
                    "failed_rules": [],
                    "missing_rules": [],
                    "errors": [str(exc)],
                }
            )

    if args.format == "json":
        for record in results:
            json_record = {
                "ticker": record.get("ticker"),
                "score": record.get("score"),
                "pass": record.get("pass"),
                "adv_jpy": _json_safe(record.get("adv_jpy")),
                "metrics": {k: _json_safe(v) for k, v in record.get("metrics", {}).items()},
                "passed_rules": record.get("passed_rules", []),
                "failed_rules": record.get("failed_rules", []),
                "missing_rules": record.get("missing_rules", []),
                "errors": record.get("errors", []),
            }
            print(json.dumps(json_record, ensure_ascii=False))
    else:
        for record in results:
            print(format_text_record(record))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
