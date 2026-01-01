import argparse
import json
import logging
from math import ceil
from typing import List, Sequence

from fetch_jp_list import DEFAULT_PAGES, fetch_jpx_list
from jf_screener import DEFAULT_MIN_SCORE, _json_safe, analyze_ticker, format_text_record


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """CLI引数を定義し、解析結果を返す。"""
    parser = argparse.ArgumentParser(
        description="Fetch JPX listings and run the fundamental screener for each ticker.",
    )
    parser.add_argument(
        "--page",
        action="append",
        help="Override listing page URL(s). Can be passed multiple times.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit the number of tickers to screen (useful for quick tests).",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (text or json lines).",
    )
    parser.add_argument(
        "--min-score",
        type=int,
        default=DEFAULT_MIN_SCORE,
        help="Minimum score to pass (0-100).",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail if 3 or more key metrics are missing (ROE, OpMargin, EquityRatio, CFO, PER, PBR).",
    )
    parser.add_argument(
        "--log-level",
        choices=["ERROR", "WARNING", "INFO", "DEBUG"],
        default="INFO",
        help="Logging level.",
    )
    return parser.parse_args(argv)


def screen_tickers(tickers: List[dict], args: argparse.Namespace, logger: logging.Logger) -> List[dict]:
    """ティッカーごとにスクリーニングし、結果レコードのリストを返す。"""
    results: List[dict] = []

    for entry in tickers:
        ticker = entry["Ticker"]
        name = entry.get("Name", "")
        code = entry.get("Code", "")

        try:
            record = analyze_ticker(
                ticker,
                timeout=10.0,
                min_score=args.min_score,
                strict=args.strict,
                logger=logger,
            )
        except Exception as exc:  # pragma: no cover - defensive
            logger.exception("Unhandled error for %s", ticker)
            record = {
                "ticker": ticker,
                "score": 0,
                "pass": False,
                "metrics": {},
                "passed_rules": [],
                "failed_rules": [],
                "missing_rules": [],
                "errors": [str(exc)],
            }

        record["code"] = code
        record["name"] = name
        results.append(record)

    return results


def select_top_percent(records: List[dict], percent: float) -> List[dict]:
    """スコア上位percent%のレコードを返す（最低1件）。"""
    if not records:
        return []
    count = max(1, ceil(len(records) * percent / 100))
    sorted_records = sorted(records, key=lambda r: r.get("score", 0), reverse=True)
    return sorted_records[:count]


def emit_records(records: List[dict], output_format: str) -> None:
    """指定フォーマットでレコードを標準出力に表示する。"""
    for record in records:
        code = record.get("code", "")
        name = record.get("name", "")

        if output_format == "json":
            json_record = {
                "code": code,
                "name": name,
                "ticker": record.get("ticker"),
                "score": record.get("score"),
                "pass": record.get("pass"),
                "metrics": {k: _json_safe(v) for k, v in record.get("metrics", {}).items()},
                "passed_rules": record.get("passed_rules", []),
                "failed_rules": record.get("failed_rules", []),
                "missing_rules": record.get("missing_rules", []),
                "errors": record.get("errors", []),
            }
            print(json.dumps(json_record, ensure_ascii=False))
        else:
            prefix = f"{code} {name}".strip()
            print(f"{prefix} | {format_text_record(record)}")


def main(argv: Sequence[str] | None = None) -> int:
    """JPXリスト取得→スクリーニング実行→上位10%を表示するCLIエントリーポイント。"""
    args = parse_args(argv)

    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s:%(message)s")
    logger = logging.getLogger("fetch_and_screen")

    pages = args.page if args.page else DEFAULT_PAGES
    listing_df = fetch_jpx_list(pages)

    tickers = listing_df.to_dict(orient="records")
    if args.limit:
        tickers = tickers[: args.limit]

    logger.info("Screening %d tickers", len(tickers))
    records = screen_tickers(tickers, args, logger)

    top_records = select_top_percent(records, 10.0)
    logger.info("Displaying top %d of %d records (top 10%%)", len(top_records), len(records))

    emit_records(top_records, args.format)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
