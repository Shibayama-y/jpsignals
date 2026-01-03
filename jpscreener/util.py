import json
import logging
import os
from pathlib import Path
from typing import Iterable, List, Sequence


def ensure_dir(path: Path) -> None:
    """Create parent directory if needed."""
    path.parent.mkdir(parents=True, exist_ok=True)


def unique_preserve(seq: Iterable[str]) -> List[str]:
    """Return unique items preserving order."""
    seen = set()
    out: List[str] = []
    for item in seq:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def load_lines(path: Path) -> List[str]:
    """Load non-empty, non-comment lines; supports CSV-like lines by taking the last field."""
    if not path.exists():
        raise FileNotFoundError(path)

    lines = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        stripped = raw.strip()
        if not stripped or stripped.startswith("#"):
            continue
        parts = [p.strip() for p in stripped.split(",") if p.strip()]
        target = parts[-1] if parts else ""
        if target:
            lines.append(target)
    return unique_preserve(lines)


def dump_json(path: Path, data) -> None:
    ensure_dir(path)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def configure_logging(level: str | int = "INFO", name: str = "jpscreener") -> logging.Logger:
    logging.basicConfig(level=level, format="%(levelname)s:%(message)s")
    return logging.getLogger(name)


def parse_int(value: str | None, default: int) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except Exception:
        return default


def normalize_ticker(ticker: str) -> str:
    stripped = ticker.strip()
    if stripped.isdigit() and len(stripped) == 4:
        return f"{stripped}.T"
    return stripped


def resolve_tickers(inputs: Sequence[str]) -> List[str]:
    return [normalize_ticker(t) for t in unique_preserve(inputs) if t.strip()]
