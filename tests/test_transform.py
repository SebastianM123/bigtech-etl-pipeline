"""Unit tests for transform_stock_data."""

import pandas as pd
import numpy as np
import pytest
import pandera.errors

from transform.transform_stock_data import transform_to_silver, transform_to_gold


# --- Silver layer ---


class TestTransformToSilver:

    def test_removes_duplicates(self, sample_bronze_df):
        result = transform_to_silver(sample_bronze_df)
        aapl_2024_01_02 = result[
            (result["ticker"] == "AAPL")
            & (result["date"] == pd.Timestamp("2024-01-02"))
        ]
        assert len(aapl_2024_01_02) == 1

    def test_total_row_count_after_dedup(self, sample_bronze_df):
        result = transform_to_silver(sample_bronze_df)
        assert len(result) == 4

    def test_drops_rows_with_null_close(self):
        df = pd.DataFrame({
            "ticker": ["AAPL", "AAPL"],
            "date":   ["2024-01-02", "2024-01-03"],
            "open":   [180.0, 181.0],
            "high":   [182.0, 183.0],
            "low":    [179.0, 180.0],
            "close":  [181.0, np.nan],
            "volume": [50_000_000, 48_000_000],
        })
        result = transform_to_silver(df)
        assert len(result) == 1
        assert result.iloc[0]["close"] == 181.0

    def test_fills_null_volume_with_zero(self):
        df = pd.DataFrame({
            "ticker": ["AAPL"],
            "date":   ["2024-01-02"],
            "open":   [180.0],
            "high":   [182.0],
            "low":    [179.0],
            "close":  [181.0],
            "volume": [np.nan],
        })
        result = transform_to_silver(df)
        assert len(result) == 1
        assert result.iloc[0]["volume"] == 0
        assert pd.api.types.is_integer_dtype(result["volume"])

    def test_rounds_prices_to_two_decimals(self):
        df = pd.DataFrame({
            "ticker": ["AAPL"],
            "date":   ["2024-01-02"],
            "open":   [180.123456],
            "high":   [182.987654],
            "low":    [179.555555],
            "close":  [181.111111],
            "volume": [50_000_000],
        })
        result = transform_to_silver(df)
        assert result.iloc[0]["open"] == 180.12
        assert result.iloc[0]["high"] == 182.99
        assert result.iloc[0]["low"] == 179.56
        assert result.iloc[0]["close"] == 181.11

    def test_output_is_sorted_by_ticker_then_date(self, sample_bronze_df):
        result = transform_to_silver(sample_bronze_df)
        expected_order = sorted(list(zip(result["ticker"], result["date"])))
        actual_order = list(zip(result["ticker"], result["date"]))
        assert actual_order == expected_order

    def test_rejects_unknown_ticker(self):
        """Schema validation must reject tickers outside the allowed list."""
        df = pd.DataFrame({
            "ticker": ["NOT_A_REAL_TICKER"],
            "date":   ["2024-01-02"],
            "open":   [180.0],
            "high":   [182.0],
            "low":    [179.0],
            "close":  [181.0],
            "volume": [50_000_000],
        })
        with pytest.raises(pandera.errors.SchemaError):
            transform_to_silver(df)


# --- Gold layer ---


class TestTransformToGold:

    def test_returns_two_tables(self, sample_silver_df):
        result = transform_to_gold(sample_silver_df)
        assert "daily_metrics" in result
        assert "company_summary" in result

    def test_daily_metrics_has_expected_columns(self, sample_silver_df):
        result = transform_to_gold(sample_silver_df)
        expected_metrics = {
            "daily_return_pct", "ma_7d", "ma_30d",
            "volatility_30d", "daily_range_pct", "above_ma30",
        }
        actual = set(result["daily_metrics"].columns)
        assert expected_metrics.issubset(actual)

    def test_first_day_per_ticker_has_null_return(self, sample_silver_df):
        """First trading day per ticker has no previous close, so return is NaN."""
        result = transform_to_gold(sample_silver_df)
        daily = result["daily_metrics"].sort_values(["ticker", "date"])
        first_rows = daily.groupby("ticker").head(1)
        assert first_rows["daily_return_pct"].isna().all()

    def test_daily_return_calculation(self):
        """Verify (close_t - close_t-1) / close_t-1 * 100."""
        df = pd.DataFrame({
            "ticker": ["AAPL", "AAPL", "AAPL"],
            "date":   pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"]),
            "open":   [100.0, 100.0, 100.0],
            "high":   [110.0, 110.0, 110.0],
            "low":    [90.0, 90.0, 90.0],
            "close":  [100.0, 110.0, 99.0],
            "volume": [1_000_000, 1_000_000, 1_000_000],
        })
        result = transform_to_gold(df)["daily_metrics"]
        returns = result.sort_values("date")["daily_return_pct"].tolist()
        assert pd.isna(returns[0])
        assert returns[1] == 10.0
        assert returns[2] == -10.0

    def test_company_summary_one_row_per_ticker(self, sample_silver_df):
        result = transform_to_gold(sample_silver_df)
        summary = result["company_summary"]
        assert len(summary) == 2

    def test_above_ma30_is_binary(self, sample_silver_df):
        result = transform_to_gold(sample_silver_df)
        daily = result["daily_metrics"]
        assert set(daily["above_ma30"].unique()).issubset({0, 1})
