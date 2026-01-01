import argparse
import io
import re
import sys
import urllib.parse
from typing import Iterable

import pandas as pd
import requests

COLUMNS = ["Code", "Name", "Ticker"]
DEFAULT_PAGES = [
    "https://www.jpx.co.jp/english/markets/statistics-equities/misc/01.html",
    "https://www.jpx.co.jp/markets/statistics-equities/misc/01.html",
]
DIRECT_FALLBACKS = [
    "https://view.officeapps.live.com/op/view.aspx?src=https%3A%2F%2Fwww.jpx.co.jp%2Fmarkets%2Fstatistics-equities%2Fmisc%2Ftvdivq0000001vg2-att%2Fdata_j.xls&wdOrigin=BROWSELINK",
    "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls",
]
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; JPXListFetcher/1.0)"
}


def _flatten_columns(df: pd.DataFrame) -> list[str]:
    """Normalize DataFrame columns to plain strings (flatten MultiIndex)."""
    if isinstance(df.columns, pd.MultiIndex):
        return [" ".join([str(x) for x in tup if str(x) != "nan"]).strip() for tup in df.columns]
    return [str(c) for c in df.columns]


def _ensure_headers(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure the DataFrame has proper headers by scanning the first few rows for 'コード'/'銘柄名'."""
    flat_cols = _flatten_columns(df)
    # Case 1: already have headers
    if {"コード", "銘柄名"}.issubset(set(flat_cols)):
        df.columns = flat_cols
        return df

    # Case 2: find a row that contains the header markers
    for i in range(min(len(df), 10)):
        row = df.iloc[i].astype(str).str.strip()
        if {"コード", "銘柄名"}.issubset(set(row)):
            df = df.copy()
            df.columns = row
            df = df.drop(index=df.index[: i + 1]).reset_index(drop=True)
            return df

    # Fallback: keep flattened columns
    df.columns = flat_cols
    return df


def resolve_listing_url(pages: Iterable[str]) -> str:
    """Find the latest JPX listing Excel link from known pages or fallbacks."""
    pattern = re.compile(r"href=\"(?P<href>[^\"]*data_j\\.(?:xls|xlsx))\"", re.IGNORECASE)
    for page in pages:
        try:
            resp = requests.get(page, headers=HEADERS, timeout=30)
            resp.raise_for_status()
        except Exception:
            continue
        match = pattern.search(resp.text)
        if not match:
            continue
        href = match.group("href")
        return urllib.parse.urljoin(page, href)
    for url in DIRECT_FALLBACKS:
        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            return url
        except Exception:
            continue
    raise SystemExit("Could not locate JPX listing file link on known pages.")


def _extract_src_if_office_viewer(url: str) -> str:
    """If the link is the Microsoft Office viewer, pull out the original file URL."""
    parsed = urllib.parse.urlparse(url)
    if "view.officeapps.live.com" in parsed.netloc:
        qs = urllib.parse.parse_qs(parsed.query)
        if "src" in qs and qs["src"]:
            return qs["src"][0]
    return url


def read_listing(url: str) -> pd.DataFrame:
    """Download and read the JPX listing Excel (xls/xlsx)."""
    url = _extract_src_if_office_viewer(url)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=60)
        resp.raise_for_status()
    except Exception as exc:
        raise SystemExit(f"Failed to download JPX listing file: {exc}") from exc

    ext = url.lower().rsplit(".", 1)[-1]
    engine = "openpyxl" if ext == "xlsx" else "xlrd"
    try:
        df = pd.read_excel(io.BytesIO(resp.content), engine=engine)
    except Exception as exc:
        raise SystemExit(f"Failed to parse JPX listing file: {exc}") from exc
    return df


def fetch_jpx_list(pages: Iterable[str]) -> pd.DataFrame:
    """Resolve listing URL, load Excel, filter out ETF/ETN, and return Code/Name/Ticker."""
    url = resolve_listing_url(pages)
    df_raw = _ensure_headers(read_listing(url))
    if not {"コード", "銘柄名"}.issubset(df_raw.columns):
        raise SystemExit("Expected JPX listing to contain 'コード' and '銘柄名' columns.")

    # Filter out ETF/ETN rows if the market/product column exists
    market_col = next(
        (
            col
            for col in df_raw.columns
            if any(key in str(col) for key in ["市場・商品区分", "市場", "商品区分"])
        ),
        None,
    )
    if market_col:
        mask = ~df_raw[market_col].astype(str).str.contains("ETF・ETN", na=False)
        df_raw = df_raw[mask]

    df = df_raw[["コード", "銘柄名"]].dropna()
    df = df.rename(columns={"コード": "Code", "銘柄名": "Name"})
    codes = df["Code"].astype(str).str.extract(r"(\d+)")[0].fillna("")
    df["Code"] = codes.str.zfill(4)

    # JPXのリストに紛れ込む先頭0のコードなどを除外（yfinanceで引けないため）
    valid_mask = df["Code"].str.match(r"^[1-9]\d{3}$")
    df = df[valid_mask]

    df = df.drop_duplicates(subset="Code", keep="first")
    df["Ticker"] = df["Code"].astype(str) + ".T"
    return df[COLUMNS].reset_index(drop=True)


def main():
    """CLI entrypoint: fetch JPX listings and emit CSV to stdout or file."""
    parser = argparse.ArgumentParser(
        description=(
            "Download JPX stock listings (code/name) and emit as CSV with yfinance tickers (.T suffix)."
        ),
    )
    parser.add_argument(
        "--output",
        "-o",
        help="Path to save CSV. If omitted, prints to stdout.",
    )
    parser.add_argument(
        "--page",
        action="append",
        help="Override listing page URL(s). Can be passed multiple times.",
    )
    args = parser.parse_args()

    pages = args.page if args.page else DEFAULT_PAGES
    df = fetch_jpx_list(pages)

    if args.output:
        df.to_csv(args.output, index=False, encoding="utf-8")
    else:
        df.to_csv(sys.stdout, index=False)


if __name__ == "__main__":
    main()
