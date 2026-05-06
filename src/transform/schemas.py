"""Pandera schemas for validating data between pipeline layers.

Validation runs at the end of each transformation step. If incoming data
violates the contract (unexpected ticker, negative price, null in a required
column, etc.), the pipeline fails fast — preventing bad data from propagating
into the Gold layer.
"""

import pandera.pandas as pa
from pandera import Column, Check


ALLOWED_TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META",
    "NVDA", "TSLA", "NFLX", "AMD",   "INTC",
]


silver_schema = pa.DataFrameSchema(
    {
        "ticker": Column(str, Check.isin(ALLOWED_TICKERS)),
        "date":   Column("datetime64[ns]"),
        "open":   Column(float, Check.greater_than(0)),
        "high":   Column(float, Check.greater_than(0)),
        "low":    Column(float, Check.greater_than(0)),
        "close":  Column(float, Check.greater_than(0)),
        "volume": Column(int, Check.greater_than_or_equal_to(0)),
    },
    strict=True,
)
