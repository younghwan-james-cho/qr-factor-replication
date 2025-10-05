# src/data_ingestion.py
#
# Functions for downloading firm-level data from WRDS.
#

import logging
import sys
from pathlib import Path
from typing import List, Optional

import polars as pl
import wrds

# Import sensitive configuration.
try:
    from config.wrds_config import WRDS_USERNAME
except ImportError:
    WRDS_USERNAME = None

def setup_logger():
    """Set up a basic logger to show INFO level messages."""
    logger = logging.getLogger(__name__)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger

logger = setup_logger()

def download_jkp_char_data(
    characteristics: List[str],
    output_path: Path,
    wrds_username: Optional[str] = WRDS_USERNAME,
    start_date: Optional[str] = None,
    country: str = 'USA',
    filter_common: bool = True,
    filter_exch_main: bool = True,
    filter_primary_sec: bool = True,
    filter_obs_main: bool = True,
) -> None:
    """Connects to WRDS and downloads specified firm characteristics data."""
    logger.info("--- Starting JKP Characteristic Data Download ---")

    if not wrds_username or wrds_username == "your_username_here":
        error_msg = "WRDS_USERNAME not set. Please update config/wrds_config.py or pass it as an argument."
        logger.error(error_msg)
        raise ValueError(error_msg)

    db = None
    try:
        logger.info(f"Connecting to WRDS with username: {wrds_username}...")
        db = wrds.Connection(wrds_username=wrds_username)
        logger.info("Successfully connected to WRDS.")

        core_cols = ["eom", "id", "permno", "crsp_exchcd", "size_grp", "me", "ret_exc_lead1m"]
        
        # Ensure the list of all columns is unique.
        all_cols = list(dict.fromkeys(core_cols + characteristics))
        columns_str = ", ".join(f'"{c}"' for c in all_cols)
        
        # Build the SQL filter string.
        filters = [f"excntry = '{country}'"]
        if filter_common: filters.append("common = 1")
        if filter_exch_main: filters.append("exch_main = 1")
        if filter_primary_sec: filters.append("primary_sec = 1")
        if filter_obs_main: filters.append("obs_main = 1")
        if start_date: filters.append(f"eom >= '{start_date}'")
        filters_str = "\n            AND ".join(filters)

        query = f"SELECT {columns_str} FROM contrib.global_factor WHERE {filters_str}"
        logger.info(f"Executing query for characteristics: {characteristics}")
        
        pandas_df = db.raw_sql(query, date_cols=["eom"])
        logger.info("Query executed successfully. Fetched %d rows and %d columns.", pandas_df.shape[0], pandas_df.shape[1])

    except Exception as e:
        logger.error("An error occurred during WRDS data download.", exc_info=True)
        raise e
    finally:
        if db:
            db.close()
            logger.info("WRDS connection closed.")

    try:
        polars_df = pl.from_pandas(pandas_df)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        polars_df.write_parquet(output_path)
        logger.info("Data saved successfully to %s. Shape: %s", output_path, polars_df.shape)
    except Exception as e:
        logger.error("Failed to convert or save data.", exc_info=True)
        raise e

    logger.info("--- Characteristic Data Download Complete ---")