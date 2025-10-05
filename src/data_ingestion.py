# src/data_ingestion.py
"""
Functions for downloading firm-level characteristics for factor construction
from the Wharton Research Data Services (WRDS) JKP database.
"""

import logging
import sys
from pathlib import Path
from typing import List, Optional

import polars as pl
import wrds

# Import the username from our secure configuration file
try:
    from config.wrds_config import WRDS_USERNAME
except ImportError:
    WRDS_USERNAME = None

# --- Logger Setup ---
# Why: A dedicated setup function makes our logging configuration reusable and clean.
def setup_logger():
    """Set up a basic logger to show INFO level messages."""
    logger = logging.getLogger(__name__)
    if not logger.handlers: # Avoid adding handlers multiple times
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
    """Connects to WRDS and downloads specified firm characteristics data.

    This function queries the JKP global factor database ('contrib.global_factor'),
    applies standard academic filters, and saves the resulting data as a Parquet file.

    Args:
        characteristics (List[str]): List of firm characteristics to download (e.g., 'be_me').
        output_path (Path): The local file path to save the downloaded data.
        wrds_username (Optional[str], optional): WRDS username. Defaults to WRDS_USERNAME from config.
        start_date (Optional[str], optional): Start date for the data in 'YYYY-MM-DD' format. Defaults to None.
        country (str, optional): Country code to filter data for. Defaults to 'USA'.
        filter_common (bool, optional): If True, filter for common stocks (common=1). Defaults to True.
        filter_exch_main (bool, optional): If True, filter for prominent exchanges (exch_main=1). Defaults to True.
        filter_primary_sec (bool, optional): If True, filter for primary listings (primary_sec=1). Defaults to True.
        filter_obs_main (bool, optional): If True, use the main observation per security-month (obs_main=1). Defaults to True.

    Returns:
        None: The function saves the data to a file and does not return anything.

    Raises:
        ValueError: If the WRDS username is not provided.
        Exception: Propagates exceptions from wrds connection or query execution.
    """
    logger.info("--- Starting JKP Characteristic Data Download ---")

    if not wrds_username or wrds_username == "your_username_here":
        error_msg = "WRDS_USERNAME not set. Please update config/wrds_config.py or pass it as an argument."
        logger.error(error_msg)
        raise ValueError(error_msg)

    db = None  # Initialize db to None
    try:
        logger.info(f"Connecting to WRDS with username: {wrds_username}...")
        db = wrds.Connection(wrds_username=wrds_username)
        logger.info("Successfully connected to WRDS.")

        # --- Dynamically Build the SQL Query ---
        core_cols = ["eom", "id", "permno", "size_grp", "me", "ret_exc_lead1m"]
        unique_chars = sorted([char for char in characteristics if char not in core_cols])
        all_cols = core_cols + unique_chars
        columns_str = ", ".join(f'"{c}"' for c in all_cols) # Use quotes for safety

        # Build filter conditions
        filters = [f"excntry = '{country}'"]
        if filter_common:
            filters.append("common = 1")
        if filter_exch_main:
            filters.append("exch_main = 1")
        if filter_primary_sec:
            filters.append("primary_sec = 1")
        if filter_obs_main:
            filters.append("obs_main = 1")
        if start_date:
            filters.append(f"eom >= '{start_date}'")
        
        filters_str = "\n            AND ".join(filters)

        query = f"""
            SELECT {columns_str}
            FROM contrib.global_factor
            WHERE {filters_str}
        """
        logger.info(f"Executing query for characteristics: {unique_chars}")
        
        pandas_df = db.raw_sql(query, date_cols=["eom"])
        logger.info("Query executed successfully. Fetched %d rows and %d columns.", pandas_df.shape[0], pandas_df.shape[1])

    except Exception as e:
        logger.error("An error occurred during WRDS data download.", exc_info=True)
        raise e  # Re-raise the exception after logging
    finally:
        if db:
            db.close()
            logger.info("WRDS connection closed.")

    # --- Convert to Polars and Save ---
    try:
        polars_df = pl.from_pandas(pandas_df)
        logger.info("Converted pandas DataFrame to Polars DataFrame.")

        output_path.parent.mkdir(parents=True, exist_ok=True)
        polars_df.write_parquet(output_path)
        logger.info("Data saved successfully to %s. Shape: %s", output_path, polars_df.shape)
    except Exception as e:
        logger.error("Failed to convert or save data.", exc_info=True)
        raise e

    logger.info("--- Characteristic Data Download Complete ---")


if __name__ == "__main__":
    import datetime
    
    project_root = Path(__file__).resolve().parent.parent
    
    start_date_str = '2020-01-01'
    end_date_str = datetime.date.today().strftime('%Y-%m-%d')
    
    start_fname = datetime.datetime.strptime(start_date_str, '%Y-%m-%d').strftime('%Y%m')
    end_fname = datetime.datetime.strptime(end_date_str, '%Y-%m-%d').strftime('%Y%m')

    output_filename = f"jkp_char_usa_{start_fname}_{end_fname}.parquet"
    output_file_path = project_root / "data" / "raw" / "char" / output_filename

    characteristics_to_download = [
        "be_me",    # Value (HML)
        "ope_be",   # Profitability (RMW)
        "at_gr1",   # Investment (CMA)
        "ret_12_1", # Momentum (MOM)
        "ret_1_0",  # Short-Term Reversal (STR)
    ]

    try:
        download_jkp_char_data(
            characteristics=characteristics_to_download,
            output_path=output_file_path,
            start_date=start_date_str,
        )
    except Exception:
        logger.critical("Script execution failed. Please check the logs above for details.")
        sys.exit(1) # Exit with a non-zero code to indicate failure