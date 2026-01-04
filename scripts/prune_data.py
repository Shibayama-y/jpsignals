from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Sequence

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from jpscreener.util import configure_logging


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prune old daily run/report files.")
    parser.add_argument("--runs-dir", default="data/runs/daily", help="Directory holding daily run artifacts.")
    parser.add_argument("--reports-dir", default="data/reports/daily", help="Directory holding daily reports.")
    parser.add_argument("--keep-days", type=int, default=90, help="Days to keep (inclusive).")
    parser.add_argument("--dry-run", action="store_true", help="List files to delete without removing.")
    parser.add_argument("--log-level", choices=["ERROR", "WARNING", "INFO", "DEBUG"], default="INFO")
    return parser.parse_args(argv)


def parse_date_from_name(path: Path) -> date | None:
    name = path.name
    try:
        return date.fromisoformat(name[:10])
    except Exception:
        return None


def prune_dir(directory: Path, cutoff: date, dry_run: bool, logger) -> int:
    if not directory.exists():
        return 0
    removed = 0
    for path in directory.iterdir():
        if not path.is_file():
            continue
        asof = parse_date_from_name(path)
        if not asof or asof >= cutoff:
            continue
        logger.info("Pruning %s", path)
        removed += 1
        if not dry_run:
            try:
                path.unlink()
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning("Failed to delete %s: %s", path, exc)
    return removed


def prune(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    logger = configure_logging(args.log_level, "prune_data")
    cutoff = date.today() - timedelta(days=args.keep_days)

    total_removed = 0
    total_removed += prune_dir(Path(args.runs_dir), cutoff, args.dry_run, logger)
    total_removed += prune_dir(Path(args.reports_dir), cutoff, args.dry_run, logger)

    logger.info("Removed %d files (cutoff %s)%s", total_removed, cutoff, " [dry-run]" if args.dry_run else "")
    return 0


def main() -> None:  # pragma: no cover - CLI wrapper
    raise SystemExit(prune())


if __name__ == "__main__":
    main()
