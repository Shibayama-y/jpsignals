import argparse
import io
import re
import sys
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests
from pypdf import PdfReader

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.fetch_jp_list import DEFAULT_PAGES as JPX_PAGES, fetch_jpx_list  # noqa: E402

USER_AGENT = "Mozilla/5.0 (compatible; JPIndexFetcher/1.0; +https://indexes.nikkei.co.jp/)"
DEFAULT_TIMEOUT = 30
COLUMNS = ["Index", "Code", "Name", "Ticker"]

INDEX_SOURCES: dict[str, list[str]] = {
    "nikkei225": [
        "https://indexes.nikkei.co.jp/nkave/archives/file/nikkei_stock_average_weight_jp.csv",
    ],
    "jpx_nikkei400": [
        "https://www.jpx.co.jp/english/markets/indices/jpx-nikkei400/tvdivq00000031dd-att/400_e.pdf",
    ],
}


def read_csv_bytes(data: bytes) -> pd.DataFrame:
    """Try multiple encodings to read Nikkei component CSV bytes."""
    errors: list[str] = []
    for enc in ("shift_jis", "cp932", "utf-8-sig", "utf-8"):
        try:
            return pd.read_csv(io.BytesIO(data), encoding=enc)
        except Exception as exc:  # pragma: no cover - defensive decoding
            errors.append(f"{enc}: {exc}")
            continue
    raise SystemExit("Failed to decode CSV with tried encodings:\n" + "\n".join(errors))


def download_first(urls: Iterable[str], cookies: dict[str, str] | None, timeout: int) -> bytes:
    """Download from the first reachable URL in the list."""
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/csv,application/csv,application/pdf,application/octet-stream,*/*;q=0.8",
        "Referer": "https://indexes.nikkei.co.jp/",
    }
    for url in urls:
        try:
            resp = requests.get(url, headers=headers, cookies=cookies, timeout=timeout)
            if resp.status_code == 200 and resp.content:
                return resp.content
            detail = f"status={resp.status_code}"
            if resp.status_code == 403:
                detail += " (forbidden; Cloudflare or cookie may be required)"
            print(f"Warning: fetch failed for {url} -> {detail}", file=sys.stderr)
        except Exception as exc:  # pragma: no cover - network errors
            print(f"Warning: fetch failed for {url}: {exc}", file=sys.stderr)
    raise SystemExit("All candidate URLs failed; cannot download constituents.")


def _choose_column(df: pd.DataFrame, keywords: list[str], default_first: bool = False) -> str | None:
    for col in df.columns:
        col_str = str(col)
        if any(key.lower() in col_str.lower() for key in keywords):
            return col
    return df.columns[0] if default_first and len(df.columns) > 0 else None


def normalize_components(df: pd.DataFrame, index_label: str) -> pd.DataFrame:
    """Extract Code/Name/Ticker columns from a raw CSV dataframe."""
    code_col = _choose_column(df, ["コード", "code", "銘柄コード", "コード番号"], default_first=True)
    name_col = _choose_column(df, ["銘柄名", "company", "name", "銘柄"])
    if name_col is None and len(df.columns) > 1:
        name_col = df.columns[1]
    if code_col is None or name_col is None:
        cols = ", ".join(str(c) for c in df.columns)
        raise SystemExit(f"Could not locate code/name columns in CSV. Columns: {cols}")

    out = df[[code_col, name_col]].copy()
    out.columns = ["Code", "Name"]
    out["Code"] = (
        out["Code"]
        .astype(str)
        .str.extract(r"(\d+)", expand=False)
        .fillna("")
        .str.zfill(4)
    )
    out = out[out["Code"].str.match(r"^\d{4}$")]
    out = out.dropna(subset=["Name"])
    out["Ticker"] = out["Code"] + ".T"
    out["Index"] = index_label
    return out[COLUMNS].reset_index(drop=True)


def _extract_components_from_pdf_bytes(data: bytes, index_label: str) -> pd.DataFrame:
    reader = PdfReader(io.BytesIO(data))
    text = "\n".join(page.extract_text() or "" for page in reader.pages)

    records: list[dict] = []
    seen: set[str] = set()
    market_tokens = {"Prime", "Standard", "Growth"}

    for line in text.splitlines():
        line = line.strip()
        match = re.match(r"^(?P<code>\d{4})\s+(?P<body>.+)$", line)
        if not match:
            continue
        code = match.group("code")
        if code in seen:
            continue

        tokens = match.group("body").split()
        if tokens and tokens[-1].isdigit():
            tokens = tokens[:-1]
        if tokens and tokens[0] in market_tokens:
            tokens = tokens[1:]

        name = " ".join(tokens).strip()
        records.append({"Code": code, "Name": name})
        seen.add(code)

    df = pd.DataFrame(records)
    if df.empty:
        raise SystemExit("Failed to parse any components from PDF.")

    df["Ticker"] = df["Code"] + ".T"
    df["Index"] = index_label
    return df[COLUMNS].reset_index(drop=True)


def enrich_with_jpx_listing(df: pd.DataFrame, pages: Iterable[str]) -> pd.DataFrame:
    """Replace names/tickers using JPX master (Japanese names)."""
    try:
        jpx = fetch_jpx_list(pages)
    except Exception as exc:  # pragma: no cover - network errors
        print(f"Warning: JPX listing fetch failed; keeping original names. ({exc})", file=sys.stderr)
        return df

    name_map = jpx.set_index("Code")["Name"].to_dict()
    ticker_map = jpx.set_index("Code")["Ticker"].to_dict()
    if not name_map:
        return df

    df = df.copy()
    df["Name"] = df["Code"].map(name_map).fillna(df["Name"])
    df["Ticker"] = df["Code"].map(ticker_map).fillna(df["Ticker"])
    return df


def fetch_index(index_key: str, cookies: dict[str, str] | None, timeout: int) -> pd.DataFrame:
    urls = INDEX_SOURCES.get(index_key)
    if not urls:
        raise SystemExit(f"Unknown index key: {index_key}")
    data = download_first(urls, cookies=cookies, timeout=timeout)
    if index_key == "jpx_nikkei400" and data.startswith(b"%PDF"):
        return _extract_components_from_pdf_bytes(data, index_key)
    raw_df = read_csv_bytes(data)
    return normalize_components(raw_df, index_key)


def load_local_file(path: Path, index_label: str) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit(f"File not found: {path}")
    data = path.read_bytes()
    if path.suffix.lower() == ".pdf" or data.startswith(b"%PDF"):
        return _extract_components_from_pdf_bytes(data, index_label)
    raw_df = read_csv_bytes(data)
    return normalize_components(raw_df, index_label)


def parse_cookie_args(cookie_args: list[str] | None) -> dict[str, str] | None:
    if not cookie_args:
        return None
    cookies: dict[str, str] = {}
    for item in cookie_args:
        if "=" not in item:
            raise SystemExit(f"Cookie must be key=value format: {item}")
        key, value = item.split("=", 1)
        cookies[key.strip()] = value.strip()
    return cookies


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download JPX-Nikkei 400 and Nikkei 225 constituents from the Nikkei index site (or JPX PDF).",
    )
    parser.add_argument(
        "--index",
        choices=["both", "nikkei225", "jpx_nikkei400"],
        default="both",
        help="Which index to download (default: both).",
    )
    parser.add_argument(
        "-o",
        "--output",
        help="Optional path to write combined CSV. If omitted, prints to stdout.",
    )
    parser.add_argument(
        "--output-dir",
        help="If set, writes per-index CSVs into this directory (nikkei225.csv, jpx_nikkei400.csv).",
    )
    parser.add_argument(
        "--universe-output",
        type=Path,
        help="Optional path to write Code/Name/Ticker only (same format as data/universe.txt).",
    )
    parser.add_argument(
        "--cookie",
        action="append",
        help="Cookie in key=value format (repeatable). Useful if the site returns 403.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help=f"Request timeout in seconds (default {DEFAULT_TIMEOUT}).",
    )
    parser.add_argument(
        "--nikkei225-file",
        type=Path,
        help="Use a locally downloaded CSV for Nikkei 225 instead of fetching.",
    )
    parser.add_argument(
        "--jpx-nikkei400-file",
        type=Path,
        help="Use a locally downloaded CSV or PDF for JPX-Nikkei 400 instead of fetching.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    cookies = parse_cookie_args(args.cookie)

    targets = ["nikkei225", "jpx_nikkei400"] if args.index == "both" else [args.index]
    frames: list[pd.DataFrame] = []
    local_files = {
        "nikkei225": args.nikkei225_file,
        "jpx_nikkei400": args.jpx_nikkei400_file,
    }

    for idx in targets:
        local_path = local_files.get(idx)
        if local_path:
            print(f"Loading {idx} from {local_path}", file=sys.stderr)
            df = load_local_file(local_path, idx)
        else:
            print(f"Fetching {idx}...", file=sys.stderr)
            df = fetch_index(idx, cookies=cookies, timeout=args.timeout)
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)
    combined = enrich_with_jpx_listing(combined, pages=JPX_PAGES)

    if args.output_dir:
        outdir = Path(args.output_dir)
        outdir.mkdir(parents=True, exist_ok=True)
        for key in targets:
            df = combined[combined["Index"] == key]
            df.to_csv(outdir / f"{key}.csv", index=False)

    if args.universe_output:
        uni_path = Path(args.universe_output)
        uni_path.parent.mkdir(parents=True, exist_ok=True)
        header = "# Code,Name,Ticker\n"
        data = combined[["Code", "Name", "Ticker"]].drop_duplicates(subset="Code")
        with open(uni_path, "w", encoding="utf-8", newline="") as fh:
            fh.write(header)
            data.to_csv(fh, index=False, header=False)

    header_line = "# " + ",".join(COLUMNS)
    if args.output:
        with open(args.output, "w", encoding="utf-8", newline="") as fh:
            fh.write(header_line + "\n")
            combined.to_csv(fh, index=False, header=False)
    else:
        sys.stdout.write(header_line + "\n")
        combined.to_csv(sys.stdout, index=False, header=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
