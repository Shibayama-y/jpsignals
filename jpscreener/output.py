from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Iterable, List

from .util import ensure_dir


def write_jsonl(path: Path, records: Iterable[Dict]) -> None:
    ensure_dir(path)
    lines = [json.dumps(rec, ensure_ascii=False) for rec in records]
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def write_manifest(path: Path, data: Dict) -> None:
    ensure_dir(path)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def write_watchlist_txt(path: Path, tickers: List[str]) -> None:
    ensure_dir(path)
    path.write_text("\n".join(tickers) + ("\n" if tickers else ""), encoding="utf-8")


def write_watchlist_json(path: Path, records: List[Dict]) -> None:
    ensure_dir(path)
    path.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
