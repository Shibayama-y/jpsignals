from __future__ import annotations

import argparse
import json
import sys
import tempfile
from datetime import date
from pathlib import Path
from typing import Dict, List, Sequence

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from jpscreener.util import configure_logging, ensure_dir


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Update positions and ledger from daily signals.")
    parser.add_argument("--asof", required=True, help="Trading date in YYYY-MM-DD (local).")
    parser.add_argument("--signals", default="out/daily.jsonl", help="Input signals JSONL file.")
    parser.add_argument("--positions", default="data/state/positions.json", help="Positions JSON file.")
    parser.add_argument("--ledger-dir", default="data/state/ledger", help="Ledger directory.")
    parser.add_argument("--events-out", required=True, help="Path to write events summary JSON.")
    parser.add_argument("--default-qty", type=int, default=200, help="Default position size.")
    parser.add_argument("--run-url", default="", help="GitHub Actions run URL for traceability.")
    parser.add_argument("--strict", action="store_true", help="Fail on missing required fields.")
    parser.add_argument("--log-level", choices=["ERROR", "WARNING", "INFO", "DEBUG"], default="INFO")
    return parser.parse_args(argv)


def load_jsonl(path: Path) -> List[dict]:
    if not path.exists():
        raise FileNotFoundError(path)
    lines = path.read_text(encoding="utf-8").splitlines()
    records: List[dict] = []
    for line in lines:
        if not line.strip():
            continue
        records.append(json.loads(line))
    return records


def atomic_write_json(path: Path, data: dict) -> None:
    ensure_dir(path)
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8", dir=path.parent) as tmp:
        json.dump(data, tmp, ensure_ascii=False, indent=2)
        tmp.flush()
    Path(tmp.name).replace(path)


def load_positions(path: Path) -> dict:
    if not path.exists():
        return {
            "schema_version": 1,
            "mode": "eod_close",
            "asof_last_applied": None,
            "positions": {},
        }
    return json.loads(path.read_text(encoding="utf-8"))


def parse_date(value: str) -> date:
    return date.fromisoformat(value)


def append_ledger_events(ledger_dir: Path, asof: date, events: List[dict]) -> None:
    if not events:
        return
    ledger_name = f"ledger-{asof.strftime('%Y-%m')}.jsonl"
    ledger_path = ledger_dir / ledger_name
    ensure_dir(ledger_path)

    existing_ids: set[str] = set()
    if ledger_path.exists():
        for line in ledger_path.read_text(encoding="utf-8").splitlines():
            try:
                obj = json.loads(line)
                if "event_id" in obj:
                    existing_ids.add(str(obj["event_id"]))
            except Exception:
                continue

    lines_to_append: List[str] = []
    for evt in events:
        if evt["event_id"] in existing_ids:
            continue
        lines_to_append.append(json.dumps(evt, ensure_ascii=False))

    if lines_to_append:
        with ledger_path.open("a", encoding="utf-8") as fh:
            for line in lines_to_append:
                fh.write(line + "\n")


def compute_entry_flag(record: dict, strict: bool, notes: List[str]) -> bool:
    if "signal_entry" in record:
        return bool(record.get("signal_entry"))
    if strict:
        raise ValueError(f"signal_entry missing for {record.get('ticker')}")
    regime = record.get("regime_ok")
    setup = record.get("setup_ok")
    entry = record.get("entry_ok")
    fallback = bool(regime and setup and entry)
    if fallback:
        notes.append(f"signal_entry inferred by regime/setup/entry for {record.get('ticker')}")
    return fallback


def compute_exit_flag(record: dict, strict: bool, notes: List[str]) -> bool:
    if "exit" in record:
        return bool(record.get("exit"))
    if strict:
        raise ValueError(f"exit missing for {record.get('ticker')}")
    notes.append(f"exit missing; treated as False for {record.get('ticker')}")
    return False


def ensure_metrics(record: dict, strict: bool, notes: List[str]) -> dict:
    metrics = record.get("metrics") or {}
    if "close" not in metrics:
        if strict:
            raise ValueError(f"metrics.close missing for {record.get('ticker')}")
        notes.append(f"metrics.close missing; skipping events for {record.get('ticker')}")
    return metrics


def update_positions(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    logger = configure_logging(args.log_level, "update_positions")

    asof = parse_date(args.asof)
    signals_path = Path(args.signals)
    positions_path = Path(args.positions)
    events_path = Path(args.events_out)
    ledger_dir = Path(args.ledger_dir)

    notes: List[str] = []
    try:
        signals = load_jsonl(signals_path)
    except FileNotFoundError:
        logger.error("Signals file not found: %s", signals_path)
        return 2

    counts = {
        "processed": len(signals),
        "entry_signals": 0,
        "exit_signals": 0,
        "entries": 0,
        "exits": 0,
    }

    try:
        positions_state = load_positions(positions_path)
    except Exception as exc:  # pragma: no cover - defensive
        logger.error("Failed to load positions: %s", exc)
        return 2
    positions_state.setdefault("positions", {})

    asof_last = positions_state.get("asof_last_applied")
    if asof_last:
        try:
            last_applied = parse_date(asof_last)
            if asof <= last_applied:
                logger.info("asof %s already applied (last=%s); emitting empty events.", asof, asof_last)
                atomic_write_json(
                    events_path,
                    {
                        "asof": args.asof,
                        "run_url": args.run_url,
                        "counts": counts,
                        "entries": [],
                        "exits": [],
                        "notes": [f"skip: asof {args.asof} already applied (last {asof_last})"],
                    },
                )
                return 0
        except Exception:
            notes.append(f"Invalid asof_last_applied {asof_last}; continuing.")

    entries: List[dict] = []
    exits: List[dict] = []

    for rec in signals:
        ticker = rec.get("ticker")
        if not ticker:
            if args.strict:
                raise ValueError("ticker missing in signal record")
            notes.append("ticker missing in record; skipped")
            continue

        metrics = ensure_metrics(rec, args.strict, notes)
        if "close" not in metrics:
            continue
        close_price = metrics.get("close")
        entry_level = metrics.get("prev20_high")
        exit_level = metrics.get("prev10_low")

        try:
            entry_signal = compute_entry_flag(rec, args.strict, notes)
            exit_signal = compute_exit_flag(rec, args.strict, notes)
        except ValueError as exc:
            logger.error(str(exc))
            return 2

        if entry_signal:
            counts["entry_signals"] += 1
        if exit_signal:
            counts["exit_signals"] += 1

        pos: Dict = positions_state.get("positions", {}).get(ticker)

        if exit_signal and pos and pos.get("status") == "OPEN":
            event_id = f"{args.asof}:{ticker}:EXIT"
            exit_event = {
                "event_id": event_id,
                "asof": args.asof,
                "ticker": ticker,
                "event": "EXIT",
                "qty": pos.get("qty", args.default_qty),
                "exit_level": exit_level,
                "close": close_price,
                "reason": "exit",
                "run_url": args.run_url,
            }
            exits.append(exit_event)
            counts["exits"] += 1

            pos["status"] = "CLOSED"
            pos["exit"] = {
                "date": args.asof,
                "level": exit_level,
                "close": close_price,
                "reason": "exit",
                "run_url": args.run_url,
            }
            pos["last_price"] = close_price
            pos["last_update"] = args.asof
            positions_state["positions"][ticker] = pos

        if entry_signal and (not pos or pos.get("status") != "OPEN"):
            event_id = f"{args.asof}:{ticker}:ENTRY"
            entry_event = {
                "event_id": event_id,
                "asof": args.asof,
                "ticker": ticker,
                "event": "ENTRY",
                "qty": args.default_qty,
                "entry_level": entry_level,
                "close": close_price,
                "reason": "signal_entry",
                "run_url": args.run_url,
            }
            entries.append(entry_event)
            counts["entries"] += 1

            positions_state["positions"][ticker] = {
                "status": "OPEN",
                "qty": args.default_qty,
                "entry": {
                    "date": args.asof,
                    "level": entry_level,
                    "close": close_price,
                    "reason": "signal_entry",
                    "run_url": args.run_url,
                },
                "exit": None,
                "last_price": close_price,
                "last_update": args.asof,
            }
        elif pos:
            # Refresh price for existing position even without events
            pos["last_price"] = close_price
            pos["last_update"] = args.asof
            positions_state["positions"][ticker] = pos

    positions_state["asof_last_applied"] = args.asof

    try:
        append_ledger_events(ledger_dir, asof, entries + exits)
    except Exception as exc:  # pragma: no cover - defensive
        logger.error("Failed to append ledger: %s", exc)
        return 2

    atomic_write_json(positions_path, positions_state)
    atomic_write_json(
        events_path,
        {
            "asof": args.asof,
            "run_url": args.run_url,
            "counts": counts,
            "entries": entries,
            "exits": exits,
            "notes": notes,
        },
    )
    logger.info("Entries: %d, exits: %d", len(entries), len(exits))
    return 0


def main() -> None:  # pragma: no cover - CLI wrapper
    raise SystemExit(update_positions())


if __name__ == "__main__":
    main()
