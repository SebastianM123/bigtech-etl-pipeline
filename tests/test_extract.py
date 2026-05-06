"""Unit tests for fetch_stock_data.

Yahoo Finance is replaced with a fake fetcher in each test, so these run in
milliseconds with no network dependency.
"""

import pandas as pd
import numpy as np
import pytest

from extract.fetch_stock_data import (
    normalize_yahoo_dataframe,
    fetch_stock_data,
)


# --- Pure helper tests ---


class TestNormalizeYahooDataFrame:

    def test_renames_columns_correctly(self):
        raw = pd.DataFrame(
            {
                "Open": [100.0],
                "High": [110.0],
                "Low": [99.0],
                "Close": [105.0],
                "Volume": [1_000_000],
            },
            index=pd.DatetimeIndex(["2024-01-02"], name="Date"),
        )
        result = normalize_yahoo_dataframe(raw, ticker="AAPL")
        assert list(result.columns) == [
            "ticker", "date", "open", "high", "low", "close", "volume"
        ]

    def test_adds_ticker_column(self):
        raw = pd.DataFrame(
            {"Open": [100.0], "High": [110.0], "Low": [99.0],
             "Close": [105.0], "Volume": [1_000_000]},
            index=pd.DatetimeIndex(["2024-01-02"], name="Date"),
        )
        result = normalize_yahoo_dataframe(raw, ticker="MSFT")
        assert (result["ticker"] == "MSFT").all()


# --- fetch_stock_data tests (with fake fetcher) ---


def make_fake_yahoo_df(num_days=3):
    """Build a fake DataFrame matching what yfinance returns."""
    dates = pd.date_range("2024-01-02", periods=num_days, freq="D")
    return pd.DataFrame(
        {
            "Open":   np.linspace(100, 100 + num_days, num_days),
            "High":   np.linspace(110, 110 + num_days, num_days),
            "Low":    np.linspace(99, 99 + num_days, num_days),
            "Close":  np.linspace(105, 105 + num_days, num_days),
            "Volume": [1_000_000] * num_days,
        },
        index=pd.DatetimeIndex(dates, name="Date"),
    )


class TestFetchStockData:

    def test_returns_combined_dataframe_for_all_tickers(self):
        def fake_fetch(ticker, start, end):
            return make_fake_yahoo_df(num_days=3)

        result = fetch_stock_data(
            tickers=["AAPL", "MSFT", "GOOGL"],
            fetch_fn=fake_fetch,
        )
        assert len(result) == 9
        assert set(result["ticker"].unique()) == {"AAPL", "MSFT", "GOOGL"}

    def test_skips_tickers_with_empty_response(self):
        """Empty Yahoo responses (delisted/missing tickers) are skipped, not crashed on."""
        def fake_fetch(ticker, start, end):
            if ticker == "BROKEN":
                return pd.DataFrame()
            return make_fake_yahoo_df(num_days=2)

        result = fetch_stock_data(
            tickers=["AAPL", "BROKEN", "MSFT"],
            fetch_fn=fake_fetch,
        )
        assert set(result["ticker"].unique()) == {"AAPL", "MSFT"}

    def test_continues_after_individual_ticker_error(self):
        """A failure on one ticker must not abort the whole batch."""
        def fake_fetch(ticker, start, end):
            if ticker == "ERROR":
                raise ConnectionError("Simulated network failure")
            return make_fake_yahoo_df(num_days=2)

        result = fetch_stock_data(
            tickers=["AAPL", "ERROR", "MSFT"],
            fetch_fn=fake_fetch,
        )
        assert set(result["ticker"].unique()) == {"AAPL", "MSFT"}

    def test_raises_when_no_tickers_succeed(self):
        def fake_fetch(ticker, start, end):
            return pd.DataFrame()

        with pytest.raises(ValueError, match="No data fetched"):
            fetch_stock_data(
                tickers=["AAPL", "MSFT"],
                fetch_fn=fake_fetch,
            )

    def test_output_has_expected_columns(self):
        def fake_fetch(ticker, start, end):
            return make_fake_yahoo_df(num_days=2)

        result = fetch_stock_data(tickers=["AAPL"], fetch_fn=fake_fetch)
        assert list(result.columns) == [
            "ticker", "date", "open", "high", "low", "close", "volume"
        ]
