from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

import yfinance as yf

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from jpscreener.util import configure_logging, ensure_dir


FRESH_DAYS = 90


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build daily email report from events.")
    parser.add_argument("--asof", required=True, help="Trading date in YYYY-MM-DD (local).")
    parser.add_argument("--signals", default="out/daily.jsonl", help="Input signals JSONL.")
    parser.add_argument("--events", required=True, help="Events JSON from update_positions.")
    parser.add_argument("--positions", default="data/state/positions.json", help="Positions JSON file.")
    parser.add_argument("--company-cache", default="data/master/company_names.json", help="Company name cache.")
    parser.add_argument("--report-out", required=True, help="Markdown output path for email body.")
    parser.add_argument("--run-out", required=True, help="Path to store the day's signals copy.")
    parser.add_argument("--status", choices=["success", "failure"], required=True, help="Workflow status.")
    parser.add_argument("--run-url", default="", help="GitHub Actions run URL.")
    parser.add_argument("--max-list", type=int, default=50, help="Maximum rows per table.")
    parser.add_argument("--log-level", choices=["ERROR", "WARNING", "INFO", "DEBUG"], default="INFO")
    return parser.parse_args(argv)


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def atomic_write_text(path: Path, content: str) -> None:
    ensure_dir(path)
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8", dir=path.parent) as tmp:
        tmp.write(content)
        tmp.flush()
    Path(tmp.name).replace(path)


def atomic_write_json(path: Path, data: dict) -> None:
    ensure_dir(path)
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8", dir=path.parent) as tmp:
        json.dump(data, tmp, ensure_ascii=False, indent=2)
        tmp.flush()
    Path(tmp.name).replace(path)


def load_events(events_path: Path, logger) -> dict:
    if not events_path.exists():
        logger.warning("Events file not found: %s", events_path)
        return {
            "asof": "",
            "run_url": "",
            "counts": {"processed": 0, "entry_signals": 0, "exit_signals": 0, "entries": 0, "exits": 0},
            "entries": [],
            "exits": [],
            "notes": ["events.json missing"],
        }
    return read_json(events_path)


def load_positions(path: Path, logger) -> dict:
    if not path.exists():
        logger.warning("Positions file missing: %s", path)
        return {"positions": {}}
    try:
        return read_json(path)
    except Exception:  # pragma: no cover - defensive
        logger.warning("Failed to parse positions file; continuing without it.")
        return {"positions": {}}


def load_company_cache(path: Path) -> dict:
    if not path.exists():
        return {"schema_version": 1, "updated_at": None, "names": {}}
    return read_json(path)


def is_fresh(asof: str | None, current: datetime) -> bool:
    if not asof:
        return False
    try:
        saved = datetime.fromisoformat(asof)
    except Exception:
        return False
    return current - saved <= timedelta(days=FRESH_DAYS)


def fetch_company_name(ticker: str, logger) -> Tuple[str, str]:
    try:
        info = yf.Ticker(ticker).get_info()
        name = info.get("shortName") or info.get("longName") or info.get("name")
        if name:
            return name, "yfinance.info"
    except Exception as exc:  # pragma: no cover - network defensive
        logger.warning("Name lookup failed for %s: %s", ticker, exc)
    return ticker, "fallback"


def resolve_company_names(tickers: List[str], cache: dict, asof_str: str, logger) -> Tuple[dict, bool]:
    now = datetime.utcnow()
    names: Dict[str, str] = {}
    modified = False
    cache.setdefault("names", {})

    for ticker in tickers:
        entry = cache["names"].get(ticker)
        if entry and is_fresh(entry.get("asof"), now):
            names[ticker] = entry.get("name", ticker)
            continue
        name, source = fetch_company_name(ticker, logger)
        names[ticker] = name
        cache["names"][ticker] = {
            "name": name,
            "source": source,
            "asof": asof_str,
        }
        modified = True

    if modified:
        cache["updated_at"] = now.replace(microsecond=0).isoformat() + "Z"
    return names, modified


def render_table(headers: List[str], rows: List[List[str]]) -> str:
    if not rows:
        return "None\n"
    header_line = "| " + " | ".join(headers) + " |\n"
    sep_line = "| " + " | ".join(["---"] * len(headers)) + " |\n"
    row_lines = ["| " + " | ".join(str(item) for item in row) + " |\n" for row in rows]
    return header_line + sep_line + "".join(row_lines)


def copy_signals(src: Path, dst: Path, notes: List[str], logger) -> None:
    ensure_dir(dst)
    if src.exists():
        shutil.copyfile(src, dst)
    else:
        notes.append(f"signals missing at {src}")
        atomic_write_text(dst, "")
        logger.warning("Signals file missing: %s", src)


def build_markdown(asof: str, status: str, run_url: str, counts: dict, entries: List[dict], exits: List[dict], names: Dict[str, str], notes: List[str], max_list: int) -> str:
    entry_rows: List[List[str]] = []
    for evt in entries[:max_list]:
        ticker = evt.get("ticker", "")
        entry_rows.append(
            [
                ticker,
                names.get(ticker, ticker),
                str(evt.get("qty", "")),
                evt.get("entry_level", "N/A") if evt.get("entry_level") is not None else "N/A",
                evt.get("close", "N/A"),
                "",
            ]
        )
    exit_rows: List[List[str]] = []
    for evt in exits[:max_list]:
        ticker = evt.get("ticker", "")
        exit_rows.append(
            [
                ticker,
                names.get(ticker, ticker),
                str(evt.get("qty", "")),
                evt.get("exit_level", "N/A") if evt.get("exit_level") is not None else "N/A",
                evt.get("close", "N/A"),
                "",
            ]
        )

    markdown = []
    markdown.append(f"Daily Run: {asof} (UTC)")
    markdown.append(f"Status: {status}")
    markdown.append(f"Run URL: {run_url or 'N/A'}")
    markdown.append("")
    markdown.append("Summary")
    markdown.append(f"- processed: {counts.get('processed', 0)}")
    markdown.append(f"- entry_signals: {counts.get('entry_signals', 0)}")
    markdown.append(f"- exit_signals: {counts.get('exit_signals', 0)}")
    markdown.append(f"- entries: {counts.get('entries', 0)}")
    markdown.append(f"- exits: {counts.get('exits', 0)}")
    markdown.append("")
    markdown.append("Entry Signals")
    markdown.append(render_table(["Ticker", "Company", "Qty", "Entry Level(prev20_high)", "Close", "Notes"], entry_rows))
    if len(entries) > max_list:
        markdown.append(f"- truncated entries to first {max_list} rows")
    markdown.append("")
    markdown.append("Exit Signals")
    markdown.append(render_table(["Ticker", "Company", "Qty", "Exit Level(prev10_low)", "Close", "Notes"], exit_rows))
    if len(exits) > max_list:
        markdown.append(f"- truncated exits to first {max_list} rows")
    if notes:
        markdown.append("")
        markdown.append("Notes")
        for n in notes:
            markdown.append(f"- {n}")
    markdown.append("")
    return "\n".join(markdown)


def build_daily_report(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    logger = configure_logging(args.log_level, "build_daily_report")

    events_path = Path(args.events)
    positions_path = Path(args.positions)
    company_cache_path = Path(args.company_cache)
    report_out = Path(args.report_out)
    run_out = Path(args.run_out)
    signals_path = Path(args.signals)

    events = load_events(events_path, logger)
    positions = load_positions(positions_path, logger)  # noqa: F841  # reserved for future use

    counts = events.get("counts", {}) or {"processed": 0, "entry_signals": 0, "exit_signals": 0, "entries": 0, "exits": 0}
    entries = events.get("entries", []) or []
    exits = events.get("exits", []) or []
    notes = list(events.get("notes", []) or [])
    asof = args.asof

    tickers = sorted({evt.get("ticker") for evt in entries + exits if evt.get("ticker")})
    cache = load_company_cache(company_cache_path)
    names, modified = resolve_company_names(tickers, cache, asof, logger)
    if modified:
        atomic_write_json(company_cache_path, cache)

    copy_signals(signals_path, run_out, notes, logger)

    markdown = build_markdown(
        asof=asof,
        status=args.status,
        run_url=args.run_url,
        counts=counts,
        entries=entries,
        exits=exits,
        names=names,
        notes=notes,
        max_list=args.max_list,
    )
    atomic_write_text(report_out, markdown)
    logger.info("Report written to %s", report_out)
    return 0


def main() -> None:  # pragma: no cover - CLI wrapper
    raise SystemExit(build_daily_report())


if __name__ == "__main__":
    main()
