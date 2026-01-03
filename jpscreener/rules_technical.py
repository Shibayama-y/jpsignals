from typing import Dict, List

import numpy as np


def evaluate_technical(metrics: Dict[str, float]) -> Dict[str, object]:
    passed: List[str] = []
    failed: List[str] = []
    missing: List[str] = []

    price = metrics.get("close", np.nan)
    ma20 = metrics.get("ma20", np.nan)
    ma50 = metrics.get("ma50", np.nan)
    ma200 = metrics.get("ma200", np.nan)
    high20 = metrics.get("high20", np.nan)
    low20 = metrics.get("low20", np.nan)
    prev20_high = metrics.get("prev20_high", np.nan)
    prev10_low = metrics.get("prev10_low", np.nan)

    if np.isnan(price):
        missing.append("Close")
    if np.isnan(ma20):
        missing.append("MA20")
    if np.isnan(ma50):
        missing.append("MA50")
    if np.isnan(ma200):
        missing.append("MA200")
    if np.isnan(high20):
        missing.append("High20")
    if np.isnan(low20):
        missing.append("Low20")
    if np.isnan(prev20_high):
        missing.append("Prev20High")
    if np.isnan(prev10_low):
        missing.append("Prev10Low")

    def eval_rule(name: str, condition: bool | None):
        if condition is None:
            return
        (passed if condition else failed).append(name)

    close_gt_ma20 = price > ma20 if not np.isnan(price) and not np.isnan(ma20) else None
    close_gt_ma50 = price > ma50 if not np.isnan(price) and not np.isnan(ma50) else None
    near20_high = price >= high20 * 0.98 if not np.isnan(price) and not np.isnan(high20) else None
    above20_low = price >= low20 * 1.05 if not np.isnan(price) and not np.isnan(low20) else None
    close_gt_ma200 = price > ma200 if not np.isnan(price) and not np.isnan(ma200) else None
    entry_rule = price > prev20_high if not np.isnan(price) and not np.isnan(prev20_high) else None
    ma50_gt_ma200 = ma50 > ma200 if not np.isnan(ma50) and not np.isnan(ma200) else None
    exit_rule = price < prev10_low if not np.isnan(price) and not np.isnan(prev10_low) else None

    eval_rule("Close>MA20", close_gt_ma20)
    eval_rule("Close>MA50", close_gt_ma50)
    eval_rule("Near20High", near20_high)
    eval_rule("Above20Low", above20_low)
    eval_rule("Close>MA200", close_gt_ma200)
    eval_rule("MA50>MA200", ma50_gt_ma200)
    eval_rule("Close>Prev20High", entry_rule)
    eval_rule("Close<Prev10Low", exit_rule)

    regime_ok = bool(close_gt_ma200 and ma50_gt_ma200)
    setup_ok = bool(close_gt_ma20 and near20_high and above20_low)
    entry_ok = bool(entry_rule)
    exit_ok = bool(exit_rule)
    signal_entry = bool(entry_ok and regime_ok and setup_ok)
    return {
        "passed": passed,
        "failed": failed,
        "missing": missing,
        "regime_ok": regime_ok,
        "setup_ok": setup_ok,
        "entry_ok": entry_ok,
        "signal_entry": signal_entry,
        "exit": exit_ok,
    }
