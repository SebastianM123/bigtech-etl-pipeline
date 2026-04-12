"""
Extract module — fetches daily stock data for US Big Tech companies
from Yahoo Finance and saves raw snapshots to the Bronze layer.
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
    "AAPL", "MSFT", "GOOGL", "AMZN", "META",
    "NVDA", "TSLA", "NFLX", "AMD", "INTC",
]

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BRONZE_PATH = os.path.join(PROJECT_ROOT, "data", "bronze")


def fetch_stock_data(tickers=TICKERS, period_days=365):
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
            stock = yf.Ticker(ticker)
            df = stock.history(start=start_date, end=end_date)

            if df.empty:
                logger.warning(f"  No data returned for {ticker}")
                continue

            df = df.reset_index()
            df["ticker"] = ticker
            df = df.rename(columns={
                "Date": "date",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
            })
            df = df[["ticker", "date", "open", "high", "low", "close", "volume"]]

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

    # Timestamped filenames preserve historical snapshots for reprocessing
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
