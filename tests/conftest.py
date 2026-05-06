"""Shared pytest fixtures."""

import pytest
import pandas as pd


@pytest.fixture
def sample_bronze_df():
    """Small Bronze-shaped DataFrame with one duplicate row for dedup testing."""
    return pd.DataFrame({
        "ticker": ["AAPL", "AAPL", "AAPL", "MSFT", "MSFT"],
        "date":   ["2024-01-02", "2024-01-02", "2024-01-03", "2024-01-02", "2024-01-03"],
        "open":   [180.0, 180.0, 181.5, 370.0, 372.0],
        "high":   [182.0, 182.0, 183.0, 372.5, 374.0],
        "low":    [179.5, 179.5, 180.0, 369.0, 371.0],
        "close":  [181.0, 181.0, 182.5, 371.0, 373.5],
        "volume": [50_000_000, 50_000_000, 48_000_000, 30_000_000, 32_000_000],
    })


@pytest.fixture
def sample_silver_df():
    """Clean Silver-shaped DataFrame with two tickers × 10 trading days."""
    rows = []
    base_date = pd.Timestamp("2024-01-02")
    for ticker, base_price in [("AAPL", 180.0), ("MSFT", 370.0)]:
        for i in range(10):
            rows.append({
                "ticker": ticker,
                "date": base_date + pd.Timedelta(days=i),
                "open": base_price + i,
                "high": base_price + i + 2,
                "low":  base_price + i - 1,
                "close": base_price + i + 1,
                "volume": 1_000_000,
            })
    return pd.DataFrame(rows)
