import argparse
import sys
import pandas as pd
import yfinance as yf

COLUMNS = ["Open", "High", "Low", "Close", "Volume"]


def fetch_single_day(tickers: list[str], date: str, auto_adjust: bool = False) -> pd.DataFrame:
    start = pd.to_datetime(date)
    end = start + pd.Timedelta(days=1)

    results = []
    missing = []

    for ticker in tickers:
        df = yf.download(
            ticker,
            start=start,
            end=end,
            interval="1d",
            auto_adjust=auto_adjust,
            progress=False,
        )
        if df.empty:
            missing.append(ticker)
            continue

        row = df.iloc[0]
        results.append({"Ticker": ticker, **{col: row[col] for col in COLUMNS}})

    if missing:
        print(
            f"No data returned for: {', '.join(missing)} on {date}. Check trading day or ticker.",
            file=sys.stderr,
        )

    if not results:
        raise SystemExit("No data returned for any tickers; aborting.")

    return pd.DataFrame(results).set_index("Ticker")


def main():
    parser = argparse.ArgumentParser(
        description="Fetch one trading day's data for one or more Japanese stocks via yfinance.",
    )
    parser.add_argument(
        "tickers",
        nargs="+",
        help="One or more tickers with .T suffix (e.g., 7203.T 9984.T)",
    )
    parser.add_argument(
        "-d",
        "--date",
        required=True,
        help="Trading date in YYYY-MM-DD (exchange local time)",
    )
    parser.add_argument(
        "--auto-adjust",
        action="store_true",
        help="Return adjusted prices (accounts for dividends/splits)",
    )
    args = parser.parse_args()

    df = fetch_single_day(args.tickers, args.date, args.auto_adjust)
    print(df[COLUMNS])


if __name__ == "__main__":
    main()
