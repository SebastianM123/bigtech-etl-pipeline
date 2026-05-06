"""
Fetch daily stock data for US Big Tech tickers from Yahoo Finance and save
the raw response to the Bronze layer.

The Yahoo API call is isolated in `_fetch_single_ticker` so it can be replaced
with a fake fetcher in unit tests, removing the network dependency.
"""

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import os
import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

TICKERS = [
    "AAPL",   # Apple
    "MSFT",   # Microsoft
    "GOOGL",  # Alphabet
    "AMZN",   # Amazon
    "META",   # Meta
    "NVDA",   # NVIDIA
    "TSLA",   # Tesla
    "NFLX",   # Netflix
    "AMD",    # AMD
    "INTC",   # Intel
]

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BRONZE_PATH = os.path.join(PROJECT_ROOT, "data", "bronze")

COLUMN_RENAMES = {
    "Date": "date",
    "Open": "open",
    "High": "high",
    "Low": "low",
    "Close": "close",
    "Volume": "volume",
}
EXPECTED_COLUMNS = ["ticker", "date", "open", "high", "low", "close", "volume"]


# --- Helpers ---


def normalize_yahoo_dataframe(raw_df, ticker):
    """Rename Yahoo columns to lowercase, add ticker, return canonical schema."""
    df = raw_df.reset_index()
    df["ticker"] = ticker
    df = df.rename(columns=COLUMN_RENAMES)
    return df[EXPECTED_COLUMNS]


def _fetch_single_ticker(ticker, start_date, end_date):
    """Network call to Yahoo Finance — isolated so tests can replace it
    via the `fetch_fn` parameter of fetch_stock_data."""
    stock = yf.Ticker(ticker)
    return stock.history(start=start_date, end=end_date)


# --- Orchestration ---


def fetch_stock_data(tickers=TICKERS, period_days=365, fetch_fn=_fetch_single_ticker):
    """Fetch historical daily data for all tickers and combine into one DataFrame.

    `fetch_fn` is injected so tests can pass a fake fetcher and avoid hitting
    the real Yahoo Finance API.
    """
    end_date = datetime.today()
    start_date = end_date - timedelta(days=period_days)

    logger.info(
        f"Fetching data for {len(tickers)} tickers "
        f"from {start_date.date()} to {end_date.date()}"
    )

    all_data = []

    for ticker in tickers:
        try:
            logger.info(f"  Downloading: {ticker}")
            raw_df = fetch_fn(ticker, start_date, end_date)

            if raw_df.empty:
                logger.warning(f"  No data returned for {ticker}")
                continue

            df = normalize_yahoo_dataframe(raw_df, ticker)
            all_data.append(df)
            logger.info(f"  Done {ticker}: {len(df)} rows")

        except Exception as e:
            logger.error(f"  Failed to fetch {ticker}: {e}")

    if not all_data:
        raise ValueError("No data fetched for any ticker.")

    result = pd.concat(all_data, ignore_index=True)
    logger.info(f"Total rows fetched: {len(result)}")
    return result


def save_to_bronze(df):
    os.makedirs(BRONZE_PATH, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"stocks_raw_{timestamp}.csv"
    filepath = os.path.join(BRONZE_PATH, filename)

    df.to_csv(filepath, index=False)
    logger.info(f"Bronze layer saved: {filepath} ({len(df)} rows)")

    return filepath


def run_extraction():
    logger.info("=" * 50)
    logger.info("STARTING EXTRACTION (Bronze Layer)")
    logger.info("=" * 50)

    df = fetch_stock_data()
    filepath = save_to_bronze(df)

    logger.info("=" * 50)
    logger.info("EXTRACTION COMPLETE")
    logger.info("=" * 50)

    return filepath


if __name__ == "__main__":
    run_extraction()
