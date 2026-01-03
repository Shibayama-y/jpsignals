from typing import Dict, List

import numpy as np

KEY_METRICS_FOR_STRICT = ["roe", "op_margin", "equity_ratio", "cfo", "per", "pbr"]
RULE_DEFS = [
    ("ROE", "roe", lambda v: v >= 0.08),
    ("OpMargin", "op_margin", lambda v: v >= 0.05),
    ("EquityRatio", "equity_ratio", lambda v: v >= 0.30),
    ("CFO", "cfo", lambda v: v > 0),
    ("PER", "per", lambda v: v <= 20),
    ("PBR", "pbr", lambda v: v <= 1.5),
]


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


def json_safe(value: float):
    if isinstance(value, float) and np.isnan(value):
        return None
    if isinstance(value, np.floating) and np.isnan(float(value)):
        return None
    return value
