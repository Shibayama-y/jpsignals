import argparse
import json
import sys
from pathlib import Path
from typing import List, Sequence

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from jpscreener.fetch_fundamentals import DEFAULT_MIN_SCORE, analyze_fundamentals, json_record
from jpscreener.fetch_prices import fetch_price_windows
from jpscreener.output import write_jsonl
from jpscreener.universe import read_universe
from jpscreener.util import configure_logging
from jpscreener.watchlist import emit_watchlist_outputs, select_watchlist


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Weekly fundamental watchlist generator for JP stocks.")
    parser.add_argument("--universe-file", required=True, help="Path to universe file (1 ticker per line).")
    parser.add_argument("--select-top", type=int, default=50, help="Number of tickers to select.")
    parser.add_argument("--min-score", type=int, default=DEFAULT_MIN_SCORE, help="Minimum score to keep (0-100).")
    parser.add_argument("--adv-window", type=int, default=20, help="ADV lookback window (days).")
    parser.add_argument(
        "--adv-period",
        default="3mo",
        help="Period for price download (yfinance syntax, default 3mo).",
    )
    parser.add_argument(
        "--min-adv-jpy",
        type=int,
        default=50_000_000,
        help="Minimum average daily value (JPY).",
    )
    parser.add_argument("--strict", action="store_true", help="Drop tickers with many missing core KPIs.")
    parser.add_argument(
        "--format",
        choices=["json", "text"],
        default="json",
        help="Output format for stdout (json lines mirrors out/weekly_scores.jsonl).",
    )
    parser.add_argument("--outdir", default="out", help="Output directory for logs and manifests.")
    parser.add_argument("--watchlist-dir", default="data", help="Directory to write watchlist outputs.")
    parser.add_argument("--timeout", type=float, default=10.0, help="Timeout placeholder (yfinance compatibility).")
    parser.add_argument("--log-level", choices=["ERROR", "WARNING", "INFO", "DEBUG"], default="INFO")
    return parser.parse_args(argv)


def format_text_record(rec: dict) -> str:
    errors = ",".join(rec.get("errors", []))
    adv = rec.get("adv_jpy")
    adtv = rec.get("adtv_shares")
    parts = [
        rec.get("ticker", ""),
        f"score={rec.get('score')}",
        f"adv_jpy={int(adv) if adv is not None else 'NaN'}",
        f"adtv={int(adtv) if adtv is not None else 'NaN'}",
        "OK" if rec.get("status") == "OK" else rec.get("status", "ERR"),
    ]
    if errors:
        parts.append(f"errors=[{errors}]")
    return "  ".join(parts)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    logger = configure_logging(args.log_level, "weekly_watchlist")

    universe_path = Path(args.universe_file)
    tickers = read_universe(universe_path)
    if not tickers:
        print("Universe is empty; aborting with exit code 2.", file=sys.stderr)
        return 2

    logger.info("Universe size: %d", len(tickers))

    scored: List[dict] = []
    for t in tickers:
        logger.info("Analyzing fundamentals for %s...", t)
        try:
            rec = analyze_fundamentals(t, timeout=args.timeout, min_score=args.min_score, strict=args.strict, logger=logger)
            rec["status"] = "OK" if not rec.get("errors") else "ERROR"
        except Exception as exc:  # pragma: no cover - defensive
            logger.exception("Unhandled error for %s", t)
            rec = {
                "ticker": t,
                "score": 0,
                "pass": False,
                "metrics": {},
                "passed_rules": [],
                "failed_rules": [],
                "missing_rules": [],
                "errors": [str(exc)],
                "status": "ERROR",
            }
        logger.info("Completed fundamentals for %s", t)
        scored.append(rec)

    price_stats = fetch_price_windows(
        [rec["ticker"] for rec in scored],
        adv_window=args.adv_window,
        adv_period=args.adv_period,
        logger=logger,
    )

    for rec in scored:
        stats = price_stats.get(rec["ticker"])
        if stats:
            rec["adv_jpy"] = stats.adv_jpy
            rec["adtv_shares"] = stats.adtv_shares
            if stats.errors:
                rec.setdefault("errors", []).extend(stats.errors)
        else:
            rec["adv_jpy"] = None
            rec["adtv_shares"] = None
            rec.setdefault("errors", []).append("adv missing")

    selection = select_watchlist(
        scored=scored,
        select_top=args.select_top,
        min_score=args.min_score,
        min_adv_jpy=args.min_adv_jpy,
        strict=args.strict,
    )

    watchlist = selection["watchlist"]
    missing = selection["missing"]

    outdir = Path(args.outdir)
    watchlist_dir = Path(args.watchlist_dir)
    emit_watchlist_outputs(
        watchlist=watchlist,
        scored=[json_record(r) | {"adv_jpy": r.get("adv_jpy"), "adtv_shares": r.get("adtv_shares"), "status": r.get("status")} for r in scored],
        missing=missing,
        outdir=outdir,
        watchlist_dir=watchlist_dir,
        manifest_name="manifest_weekly.json",
    )

    # stdout
    if args.format == "json":
        for rec in scored:
            obj = json_record(rec)
            obj["adv_jpy"] = rec.get("adv_jpy")
            obj["adtv_shares"] = rec.get("adtv_shares")
            obj["status"] = rec.get("status")
            print(json.dumps(obj, ensure_ascii=False))
    else:
        for rec in scored:
            print(format_text_record(rec))

    logger.info("Watchlist selected: %d / %d", len(watchlist), len(scored))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
