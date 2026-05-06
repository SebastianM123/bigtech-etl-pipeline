"""
Transform stock data through Silver and Gold layers.

Silver: cleaned, deduplicated, properly typed.
Gold:   business metrics — daily returns, moving averages, volatility, rankings.

Transformation functions are pure (DataFrame in, DataFrame out) so they can
be unit-tested without filesystem or network access.
"""

import pandas as pd
import os
import logging
import glob

from transform.schemas import silver_schema

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BRONZE_PATH = os.path.join(PROJECT_ROOT, "data", "bronze")
SILVER_PATH = os.path.join(PROJECT_ROOT, "data", "silver")
GOLD_PATH = os.path.join(PROJECT_ROOT, "data", "gold")


# --- I/O helpers ---


def get_latest_bronze_file():
    """Return the path to the most recent Bronze layer CSV."""
    files = glob.glob(os.path.join(BRONZE_PATH, "stocks_raw_*.csv"))
    if not files:
        raise FileNotFoundError("No Bronze layer files found.")
    latest = max(files, key=os.path.getmtime)
    logger.info(f"Using Bronze file: {latest}")
    return latest


def load_bronze_csv(filepath):
    return pd.read_csv(filepath)


def save_silver(df):
    os.makedirs(SILVER_PATH, exist_ok=True)
    filepath = os.path.join(SILVER_PATH, "stocks_cleaned.csv")
    df.to_csv(filepath, index=False)
    logger.info(f"  Silver saved: {filepath}")
    return filepath


def save_gold(gold_tables):
    os.makedirs(GOLD_PATH, exist_ok=True)
    paths = []
    for name, df in gold_tables.items():
        filepath = os.path.join(GOLD_PATH, f"{name}.csv")
        df.to_csv(filepath, index=(name == "company_summary"))
        logger.info(f"  Gold saved: {filepath}")
        paths.append(filepath)
    return paths


# --- Transformations ---


def transform_to_silver(df):
    """Clean and standardize raw Bronze data: parse dates, deduplicate,
    drop nulls, fill volume, round prices, sort, and validate against the schema."""
    logger.info("SILVER LAYER: Starting transformations...")

    df = df.copy()
    initial_rows = len(df)
    logger.info(f"  Read {initial_rows} rows from Bronze")

    # 1. Parse dates and drop timezone info
    df["date"] = pd.to_datetime(df["date"], utc=True).dt.date
    df["date"] = pd.to_datetime(df["date"])

    df = df.drop_duplicates(subset=["ticker", "date"], keep="last")
    dupes_removed = initial_rows - len(df)
    if dupes_removed > 0:
        logger.info(f"  Removed {dupes_removed} duplicate rows")

    price_cols = ["open", "high", "low", "close"]
    nulls_before = df[price_cols].isnull().sum().sum()
    df = df.dropna(subset=price_cols)
    if nulls_before > 0:
        logger.info(f"  Dropped {nulls_before} rows with null prices")

    df["volume"] = df["volume"].fillna(0).astype("int64")

    for col in price_cols:
        df[col] = df[col].round(2)

    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)

    df = silver_schema.validate(df)

    logger.info(f"  Silver layer: {len(df)} clean rows")
    return df


def transform_to_gold(silver_df):
    """Compute business metrics from Silver data.

    Returns a dict with two DataFrames:
        - daily_metrics:   per-ticker per-day metrics (returns, moving averages, etc.)
        - company_summary: per-ticker aggregates (avg close, total volume, etc.)
    """
    logger.info("GOLD LAYER: Computing business metrics...")

    df = silver_df.copy()
    df = df.sort_values(["ticker", "date"])

    df["daily_return_pct"] = df.groupby("ticker")["close"].pct_change() * 100
    df["daily_return_pct"] = df["daily_return_pct"].round(4)

    df["ma_7d"] = (
        df.groupby("ticker")["close"]
        .transform(lambda x: x.rolling(window=7, min_periods=1).mean())
        .round(2)
    )
    df["ma_30d"] = (
        df.groupby("ticker")["close"]
        .transform(lambda x: x.rolling(window=30, min_periods=1).mean())
        .round(2)
    )

    df["volatility_30d"] = (
        df.groupby("ticker")["daily_return_pct"]
        .transform(lambda x: x.rolling(window=30, min_periods=7).std())
        .round(4)
    )

    df["daily_range_pct"] = (((df["high"] - df["low"]) / df["close"]) * 100).round(4)

    df["above_ma30"] = (df["close"] > df["ma_30d"]).astype(int)

    daily_metrics = df.copy()
    logger.info(f"  Daily metrics: {len(daily_metrics)} rows, {len(daily_metrics.columns)} columns")

    summary = df.groupby("ticker").agg(
        latest_close=("close", "last"),
        avg_close=("close", "mean"),
        max_close=("close", "max"),
        min_close=("close", "min"),
        avg_volume=("volume", "mean"),
        total_volume=("volume", "sum"),
        avg_daily_return=("daily_return_pct", "mean"),
        max_daily_return=("daily_return_pct", "max"),
        min_daily_return=("daily_return_pct", "min"),
        avg_volatility=("volatility_30d", "mean"),
        days_above_ma30=("above_ma30", "sum"),
        total_trading_days=("close", "count"),
    ).round(4)

    summary["pct_days_above_ma30"] = (
        (summary["days_above_ma30"] / summary["total_trading_days"]) * 100
    ).round(2)

    summary = summary.sort_values("avg_volatility", ascending=False)
    logger.info(f"  Company summary: {len(summary)} companies")

    return {
        "daily_metrics": daily_metrics,
        "company_summary": summary,
    }


# --- Pipeline ---


def run_transformations():
    logger.info("=" * 50)
    logger.info("STARTING TRANSFORMATIONS")
    logger.info("=" * 50)

    bronze_file = get_latest_bronze_file()

    bronze_df = load_bronze_csv(bronze_file)
    silver_df = transform_to_silver(bronze_df)
    save_silver(silver_df)

    gold_tables = transform_to_gold(silver_df)
    save_gold(gold_tables)

    logger.info("=" * 50)
    logger.info("TRANSFORMATIONS COMPLETE")
    logger.info("=" * 50)


if __name__ == "__main__":
    run_transformations()
