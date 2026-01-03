from pathlib import Path
from typing import List

from .util import load_lines, normalize_ticker, unique_preserve


def read_universe(path: Path) -> List[str]:
    tickers = [normalize_ticker(t) for t in load_lines(path)]
    return unique_preserve(tickers)


def read_watchlist(path: Path) -> List[str]:
    return read_universe(path)
