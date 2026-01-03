import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Sequence

import yfinance as yf

import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from jpscreener.fetch_fundamentals import analyze_fundamentals, json_record
from jpscreener.output import write_jsonl, write_manifest
from jpscreener.universe import read_watchlist
from jpscreener.util import configure_logging, resolve_tickers

RECENT_EARNINGS_DAYS = 7


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh fundamentals for tickers with recent earnings.")
    parser.add_argument("tickers", nargs="*", help="Optional ticker override (ignore watchlist).")
    parser.add_argument("--watchlist-file", default="data/watchlist.txt", help="Watchlist file to read.")
    parser.add_argument("--outdir", default="out", help="Output directory.")
    parser.add_argument("--days", type=int, default=RECENT_EARNINGS_DAYS, help="How many days after earnings to refresh.")
    parser.add_argument("--min-score", type=int, default=60, help="Minimum score threshold.")
    parser.add_argument("--strict", action="store_true", help="Enable strict fundamental mode.")
    parser.add_argument("--log-level", choices=["ERROR", "WARNING", "INFO", "DEBUG"], default="INFO")
    return parser.parse_args(argv)


def earnings_within(ticker: str, days: int, logger) -> bool:
    try:
        info = yf.Ticker(ticker).info or {}
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("info fetch failed for %s: %s", ticker, exc)
        return False
    ts = info.get("earningsTimestamp") or info.get("earningsTimestampStart")
    if not ts:
        return False
    try:
        dt = datetime.fromtimestamp(float(ts), tz=timezone.utc)
        return (datetime.now(tz=timezone.utc) - dt).days <= days
    except Exception:
        return False


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    logger = configure_logging(args.log_level, "post_earnings_refresh")

    tickers = resolve_tickers(args.tickers) if args.tickers else read_watchlist(Path(args.watchlist_file))
    if not tickers:
        manifest = {"status": "NO_TICKERS", "output": {}}
        write_manifest(Path(args.outdir) / "manifest_post_earnings.json", manifest)
        return 0

    to_refresh: List[str] = []
    for t in tickers:
        if earnings_within(t, args.days, logger):
            to_refresh.append(t)
    if not to_refresh:
        write_manifest(
            Path(args.outdir) / "manifest_post_earnings.json",
            {"status": "NO_RECENT_EARNINGS", "checked": len(tickers), "output": {}},
        )
        logger.info("No tickers with earnings in last %d days.", args.days)
        return 0

    records = []
    for t in to_refresh:
        rec = analyze_fundamentals(t, timeout=10.0, min_score=args.min_score, strict=args.strict, logger=logger)
        rec["refreshed_at"] = int(time.time())
        records.append(json_record(rec))

    outdir = Path(args.outdir)
    write_jsonl(outdir / "post_earnings.jsonl", records)
    write_manifest(
        outdir / "manifest_post_earnings.json",
        {
            "status": "OK",
            "refreshed": len(records),
            "output": {"post_earnings_jsonl": str((outdir / "post_earnings.jsonl").as_posix())},
        },
    )

    for rec in records:
        print(json.dumps(rec, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
