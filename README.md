# US Big Tech Stock Market ETL Pipeline

![CI](https://github.com/SebastianM123/bigtech-etl-pipeline/actions/workflows/ci.yml/badge.svg)

End-to-end ETL pipeline that fetches daily stock market data for the largest US tech companies, processes it through a medallion architecture (Bronze → Silver → Gold), and produces business-ready metrics using PySpark on Databricks with Delta Lake.

## Project Overview

This project demonstrates a production-grade data engineering workflow by building a complete ETL pipeline for US Big Tech stock market data (AAPL, MSFT, GOOGL, AMZN, META, NVDA, TSLA, NFLX, AMD, INTC). The pipeline extracts raw market data from Yahoo Finance, applies a medallion architecture to progressively clean and enrich the data, and produces analytical tables with key financial metrics.

**Why this project?** It mirrors real-world data engineering tasks: working with streaming-like time-series data, implementing layered data architectures, writing efficient distributed transformations, validating data quality between layers, and delivering business-ready datasets for analytics.

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Extraction** | Python, `yfinance` | Fetch daily OHLCV data from Yahoo Finance API |
| **Processing** | Apache Spark (PySpark) | Distributed data transformations |
| **Storage** | Delta Lake | ACID-compliant table format with versioning |
| **Platform** | Databricks (Community Edition) | Managed Spark environment |
| **Data Quality** | Pandera | Schema validation between pipeline layers |
| **Testing** | Pytest | Unit tests for transformation logic |
| **CI/CD** | GitHub Actions | Automated testing on every push |
| **Containerization** | Docker, Docker Compose | Reproducible, portable execution environment |
| **Orchestration** | Python scripts + Databricks Notebooks | Pipeline execution |
| **Version Control** | Git, GitHub | Source code management |
| **Analysis** | Spark SQL | Ad-hoc queries on Gold layer |

## Architecture

The pipeline follows the **Medallion Architecture** pattern — a standard design in modern data platforms where data flows through three progressive layers:

```
┌─────────────────┐     ┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│  Yahoo Finance  │────▶│  BRONZE LAYER   │────▶│  SILVER LAYER   │────▶│   GOLD LAYER    │
│   (yfinance)    │     │   Raw Data      │      │  Cleaned Data   │      │ Business Metrics│
└─────────────────┘     └─────────────────┘      └─────────────────┘      └─────────────────┘
                         • As-is from API        • Deduplicated          • Daily returns
                         • Timestamped files     • Type-enforced         • Moving averages
                         • No transformations    • Null handling         • 30d volatility
                                                 • Schema validated      • Company ranking
```

### Layer Responsibilities

**Bronze Layer** — *Raw ingestion*
- Stores unmodified data exactly as received from the source API
- Each extraction creates a timestamped snapshot for full reproducibility
- Acts as a single source of truth for downstream layers

**Silver Layer** — *Cleansed & conformed*
- Removes duplicates (by ticker + date)
- Handles missing values (drops rows with null prices, fills null volume with 0)
- Standardizes data types (dates, numeric precision)
- Validated against a Pandera schema before being written
- Sorted and ready for business logic

**Gold Layer** — *Business-ready analytics*
- **Daily metrics table** — per-ticker indicators using Spark Window Functions:
  - `daily_return_pct` — day-over-day percentage change
  - `ma_7d` / `ma_30d` — 7 and 30-day moving averages
  - `volatility_30d` — 30-day rolling standard deviation of returns
  - `daily_range_pct` — intraday price range
  - `above_ma30` — trend signal (price above 30-day MA)
- **Company summary table** — aggregated statistics per ticker with volatility ranking

## Project Structure

```
bigtech-etl-pipeline/
├── .github/
│   └── workflows/
│       └── ci.yml                   # GitHub Actions: run tests on every push
├── src/
│   ├── extract/
│   │   └── fetch_stock_data.py      # Extracts data from Yahoo Finance → Bronze
│   ├── transform/
│   │   ├── transform_stock_data.py  # Bronze → Silver → Gold transformations
│   │   └── schemas.py               # Pandera validation schemas
│   ├── load/                        # (Reserved for future: cloud storage load)
│   └── run_pipeline.py              # Main pipeline orchestrator
├── notebooks/
│   └── bigtech_etl_databricks.py    # PySpark pipeline on Databricks
├── data/
│   ├── bronze/                      # Raw snapshots (timestamped CSVs)
│   ├── silver/                      # Cleaned data
│   └── gold/                        # Business metrics
├── tests/
│   ├── conftest.py                  # Shared pytest fixtures
│   ├── test_extract.py              # Tests for the extraction module
│   └── test_transform.py            # Tests for Silver/Gold transformations
├── pyproject.toml                   # Project metadata & pytest configuration
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .gitignore
└── README.md
```

The project contains **two parallel implementations** of the same pipeline:
- **Local Python version** (`src/`) — uses pandas for transformations, writes CSV files
- **Databricks PySpark version** (`notebooks/`) — uses distributed Spark, writes Delta tables

This dual approach demonstrates both a lightweight local development workflow and a production-grade cloud implementation using the same medallion architecture.

## How to Run

### Prerequisites
- Python 3.9+
- Git
- Docker (optional)
- Databricks Community Edition account (optional)

### Docker (recommended)

```bash
git clone https://github.com/SebastianM123/bigtech-etl-pipeline.git
cd bigtech-etl-pipeline

docker compose up --build
```

Output files will be available in `data/bronze/`, `data/silver/`, and `data/gold/` on your local machine thanks to volume mounting.

### Local Pipeline (pandas)

```bash
git clone https://github.com/SebastianM123/bigtech-etl-pipeline.git
cd bigtech-etl-pipeline

pip install -r requirements.txt

python src/run_pipeline.py
```

After running, output files will be available in:
- `data/bronze/stocks_raw_<timestamp>.csv`
- `data/silver/stocks_cleaned.csv`
- `data/gold/daily_metrics.csv`
- `data/gold/company_summary.csv`

### Databricks Pipeline (PySpark)

1. Log in to [Databricks Community Edition](https://community.cloud.databricks.com)
2. Create a new notebook and import `notebooks/bigtech_etl_databricks.py`
3. Run all cells in order — the pipeline will create Delta tables in the default catalog:
   - `bigtech_bronze_stocks`
   - `bigtech_silver_stocks`
   - `bigtech_gold_daily_metrics`
   - `bigtech_gold_company_summary`

## Testing

The project ships with a `pytest` test suite covering both the extraction and transformation layers.

```bash
pip install pytest
pytest
```

The suite includes 20 unit tests:

- **Transformation tests** verify deduplication, null handling, rounding, sorting, daily-return math, summary aggregation, and schema rejection of unknown tickers.
- **Extraction tests** use a fake fetcher (dependency injection) to exercise the orchestration logic without hitting the real Yahoo Finance API — making the suite fast, deterministic, and offline-runnable.

Tests run automatically on every push and pull request via GitHub Actions (see `.github/workflows/ci.yml`).

## Data Quality

Data quality is enforced with **Pandera** schemas (`src/transform/schemas.py`). At the end of the Silver transformation, the DataFrame is validated against a contract that checks:

- Tickers are within the allowed whitelist
- All price columns (`open`, `high`, `low`, `close`) are positive
- `volume` is non-negative integer
- `date` is a proper datetime
- No unexpected columns are present (`strict=True`)

If any rule is violated, the pipeline fails fast with a precise error — preventing bad data from silently propagating into the Gold layer and corrupting downstream metrics.

## Key Design Decisions

### Why the Medallion Architecture?
Splitting the pipeline into Bronze/Silver/Gold layers provides clear separation of concerns and makes the pipeline debuggable and idempotent. If a bug is found in Gold logic, we can reprocess from Silver without re-fetching from the source API — saving time and reducing load on external systems.

### Why Delta Lake over plain Parquet/CSV?
Delta Lake provides ACID transactions, schema enforcement, and time travel. In a production context, this means:
- Failed writes don't corrupt data (transactional guarantees)
- Schema changes are caught early
- Historical data versions are queryable (`VERSION AS OF`)

### Why PySpark instead of pandas?
While pandas works fine for this dataset size, PySpark was chosen to reflect real-world scale and align with modern cloud data platforms. The same code pattern scales from 10 tickers to 10 million rows without modification — a critical property in production data engineering.

### Why Window Functions for metrics?
Moving averages, volatility, and daily returns are inherently sequential per ticker. Window functions (`Window.partitionBy("ticker").orderBy("date")`) express this natively in Spark, are highly optimized, and translate directly to equivalent SQL — making the code easy to review and maintain.

### Why pure functions for transformations?
The local Python pipeline keeps transformation logic (`transform_to_silver`, `transform_to_gold`) free of file I/O. I/O is handled by separate helper functions. This separation makes the core logic trivially unit-testable with hand-crafted DataFrames — no temporary files, no network mocking required for the transformation layer.

### Why a dual local + Databricks implementation?
Local development with pandas enables fast iteration and debugging without cluster startup costs. The Databricks notebook then validates the same logic works at scale with Spark. This mirrors how teams often prototype locally before deploying to cloud infrastructure.

## Sample Insights from the Gold Layer

The pipeline enables analytical queries like:

**Top 5 most volatile stocks** — ranking companies by 30-day rolling standard deviation of daily returns.

**Best/worst single-day movements** — identifying extreme market events per ticker, useful for correlating with news or earnings releases.

**Trend persistence** — the `pct_days_above_ma30` metric shows what percentage of trading days each stock spent in an uptrend, giving a long-term directional signal.

**Intraday volatility** — the `daily_range_pct` metric highlights stocks with wide intraday price swings, often indicating higher trading risk.

Example query:
```sql
SELECT ticker, latest_close, avg_volatility, pct_days_above_ma30
FROM bigtech_gold_company_summary
ORDER BY avg_volatility DESC
LIMIT 5;
```

## Future Improvements

Possible extensions to take this project closer to a full production system:

- **Orchestration** — migrate from manual notebook runs to Apache Airflow with a scheduled DAG
- **Incremental loads** — use Delta Lake `MERGE` for upsert logic instead of full overwrites
- **Azure deployment** — run the pipeline on Azure Data Lake Storage Gen2 with Azure Databricks
- **Secrets management** — integrate Azure Key Vault or Databricks Secrets for credentials

## Author

Built by **Sebastian Michulec** as a portfolio project.
