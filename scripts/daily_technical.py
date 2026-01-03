import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from jpscreener.fetch_prices import fetch_recent_prices
from jpscreener.indicators_technical import build_technical_record
from jpscreener.output import write_jsonl, write_manifest
from jpscreener.util import configure_logging, resolve_tickers


def read_pass_watchlist(path: Path) -> list[str]:
    tickers: list[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        tokens = line.replace(",", " ").split()
        if not tokens:
            continue
        ticker = tokens[0]
        status = next((t for t in tokens[1:] if t.upper() in {"PASS", "FAIL"}), None)
        if status == "PASS":
            tickers.append(ticker)
    return resolve_tickers(tickers)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Daily technical screening for watchlist tickers.")
    parser.add_argument("tickers", nargs="*", help="Optional ticker override (ignore watchlist file).")
    parser.add_argument("--watchlist-file", default="data/watchlist.txt", help="Watchlist file to read.")
    parser.add_argument("--period", default="1y", help="Price history period (yfinance syntax).")
    parser.add_argument("--outdir", default="out", help="Output directory.")
    parser.add_argument("--log-level", choices=["ERROR", "WARNING", "INFO", "DEBUG"], default="INFO")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    logger = configure_logging(args.log_level, "daily_technical")

    tickers = resolve_tickers(args.tickers) if args.tickers else read_pass_watchlist(Path(args.watchlist_file))
    if not tickers:
        outdir = Path(args.outdir)
        manifest = {"status": "NO_TICKERS", "watchlist": 0, "output": {}}
        write_manifest(outdir / "manifest_daily.json", manifest)
        logger.info("No tickers to process; exiting 0.")
        return 0

    logger.info("Processing %d tickers", len(tickers))
    prices = fetch_recent_prices(tickers, period=args.period, logger=logger)

    records = []
    for t in tickers:
        df = prices.get(t)
        record = build_technical_record(t, df)
        records.append(record)

    outdir = Path(args.outdir)
    write_jsonl(outdir / "daily.jsonl", records)
    write_manifest(
        outdir / "manifest_daily.json",
        {
            "status": "OK",
            "ticker_count": len(records),
            "output": {
                "daily_jsonl": str((outdir / "daily.jsonl").as_posix()),
            },
        },
    )

    for rec in records:
        obj = {
            "ticker": rec["ticker"],
            "entry_ok": rec["entry_ok"],
            "regime_ok": rec["regime_ok"],
            "setup_ok": rec["setup_ok"],
            "signal_entry": rec["signal_entry"],
            "exit": rec["exit"],
            "metrics": rec.get("metrics", {}),
            "passed_rules": rec.get("passed_rules", []),
            "failed_rules": rec.get("failed_rules", []),
            "missing_rules": rec.get("missing_rules", []),
        }
        print(json.dumps(obj, ensure_ascii=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
