"""
Main ETL pipeline runner.
Orchestrates the full flow: Extract (Bronze) → Transform (Silver → Gold).
"""

import sys
import os
import logging
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from extract.fetch_stock_data import run_extraction
from transform.transform_stock_data import run_transformations

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def run_pipeline():
    start_time = datetime.now()

    logger.info("=" * 60)
    logger.info("  US BIG TECH STOCK MARKET ETL PIPELINE")
    logger.info(f"  Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    try:
        logger.info("\nSTEP 1/2: EXTRACTION")
        run_extraction()

        logger.info("\nSTEP 2/2: TRANSFORMATION")
        run_transformations()

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("\n" + "=" * 60)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        logger.info(f"  Duration: {duration:.1f} seconds")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"\nPIPELINE FAILED: {e}")
        raise


if __name__ == "__main__":
    run_pipeline()
