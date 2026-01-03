from typing import Dict, List

import pandas as pd
import yfinance as yf

from .indicators_fundamental import extract_metrics
from .rules_fundamental import evaluate_rules, json_safe, score_metrics
from .util import normalize_ticker

DEFAULT_MIN_SCORE = 60


def load_statement(tkr: yf.Ticker, attr_candidates: List[str], errors: List[str]) -> pd.DataFrame:
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


def analyze_fundamentals(ticker: str, timeout: float, min_score: int, strict: bool, logger) -> Dict[str, object]:
    normalized = normalize_ticker(ticker)
    errors: List[str] = []

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
        "metrics": metrics,
        "passed_rules": rule_eval["passed"],
        "failed_rules": rule_eval["failed"],
        "missing_rules": rule_eval["missing"],
        "errors": errors,
    }
    return record


def json_record(record: Dict[str, object]) -> Dict[str, object]:
    return {
        "ticker": record.get("ticker"),
        "score": record.get("score"),
        "pass": record.get("pass"),
        "metrics": {k: json_safe(v) for k, v in record.get("metrics", {}).items()},
        "passed_rules": record.get("passed_rules", []),
        "failed_rules": record.get("failed_rules", []),
        "missing_rules": record.get("missing_rules", []),
        "errors": record.get("errors", []),
    }
