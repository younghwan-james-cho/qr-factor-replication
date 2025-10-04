# src/data_ingestion.py
"""
Functions for connecting to and downloading data from Wharton Research Data Services (WRDS).

This script is designed to be the single source for all data ingestion,
ensuring a consistent, secure, and reproducible data pipeline.
"""

import wrds
import polars as pl
from pathlib import Path

# Import the username from our secure configuration file
try:
    from config.wrds_config import WRDS_USERNAME
except ImportError:
    # Provide a default or raise an error if the config file is not found
    WRDS_USERNAME = None


def download_jkp_hml_data(output_dir: Path, wrds_username: str = WRDS_USERNAME) -> None:
    """
    Connects to WRDS and downloads the necessary data for replicating the HML factor
    from the Jensen, Kelly, and Pedersen (JKP) global factor database.

    This function applies the standard JKP screens for professional-grade research.
    The data is saved locally as a Parquet file for efficient storage and access.

    Args:
        output_dir (Path): The directory where the downloaded data will be saved.
        wrds_username (str): The username for the WRDS connection. Defaults to the
                             one in the config file.
    """
    print("--- Starting JKP HML Data Download ---")

    if not wrds_username or wrds_username == "your_username":
        print("Error: WRDS_USERNAME not set in config/wrds_config.py")
        return

    # --- 1. Securely connect to WRDS ---
    try:
        print(f"Connecting to WRDS with username: {wrds_username}...")
        db = wrds.Connection(
            wrds_username=wrds_username
        )
        print("Successfully connected to WRDS.")
    except Exception as e:
        print(f"Failed to connect to WRDS: {e}")
        return

    # --- 2. Define the SQL Query for HML Replication ---
    hml_query = """
        SELECT
            eom, id, permno,
            be_me, -- The Book-to-Market (Value) characteristic
            me,    -- Market Equity for weighting
            ret_exc_lead1m -- The forward 1-month excess return (our target)
        FROM
            contrib.global_factor
        WHERE
            /* Apply standard professional screens for US stocks */
            excntry = 'USA' AND
            common = 1 AND
            exch_main = 1 AND
            primary_sec = 1 AND
            obs_main = 1
    """

    # --- 3. Execute the query and download the data ---
    try:
        print("Executing SQL query on WRDS... (This may take several minutes)")
        # *** FIX: Use the wrds library's robust raw_sql method first ***
        # This returns a pandas DataFrame, handling type conversions correctly.
        pandas_df = db.raw_sql(hml_query, date_cols=["eom"])
        
        # *** FIX: Convert the pandas DataFrame to Polars ***
        # This is a fast, in-memory conversion.
        raw_data = pl.from_pandas(pandas_df)

        print("Query executed successfully. Data downloaded.")
        db.close()
        print("WRDS connection closed.")
    except Exception as e:
        print(f"Failed to execute query or download data: {e}")
        db.close()
        return

    # --- 4. Save the data locally ---
    output_path = output_dir / "jkp_hml_raw_data_usa.parquet"
    try:
        print(f"Saving data to {output_path}...")
        raw_data.write_parquet(output_path)
        print(f"Data saved successfully. Shape: {raw_data.shape}")
    except Exception as e:
        print(f"Failed to save data: {e}")
        return

    print("--- Data Download Complete ---")


if __name__ == "__main__":
    project_root = Path(__file__).resolve().parent.parent
    data_output_dir = project_root / "data" / "raw"
    data_output_dir.mkdir(parents=True, exist_ok=True)
    download_jkp_hml_data(output_dir=data_output_dir)