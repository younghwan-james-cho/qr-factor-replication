# src/benchmark_downloader.py
"""
Functions for downloading the pre-computed benchmark factor portfolios from the
Wharton Research Data Services (WRDS) JKP database.
"""

import wrds
import polars as pl
from pathlib import Path
from typing import List

# Import the username from our secure configuration file
try:
    from config.wrds_config import WRDS_USERNAME
except ImportError:
    WRDS_USERNAME = None


def download_jkp_benchmark_factors(
    characteristic_names: List[str],
    output_path: Path,
    wrds_username: str = WRDS_USERNAME,
) -> None:
    """
    Connects to WRDS and downloads the pre-computed monthly factor returns for a
    given list of characteristics for US stocks.

    This data is sourced from the 'contrib.global_factors_monthly' table.

    Args:
        characteristic_names (List[str]): List of characteristics to download benchmarks for (e.g., ['be_me']).
        output_path (Path): The full path where the downloaded data will be saved.
        wrds_username (str): The username for the WRDS connection.
    """
    print("--- Starting JKP Benchmark Factor Download ---")

    if not wrds_username or wrds_username == "your_username":
        print("Error: WRDS_USERNAME not set in config/wrds_config.py")
        return

    # --- Securely connect to WRDS ---
    try:
        print(f"Connecting to WRDS with username: {wrds_username}...")
        db = wrds.Connection(
            wrds_username=wrds_username)
        print("Successfully connected to WRDS.")
    except Exception as e:
        print(f"Failed to connect to WRDS: {e}")
        return

    # --- Define the SQL Query ---
    # The 'in' clause allows us to download multiple factors at once if needed in the future.
    char_list_str = "('" + "', '".join(characteristic_names) + "')"
    
    # CORRECTED TABLE NAME: from 'contrib.global_factor_returns_monthly' to 'contrib.global_factors_monthly'
    query = f"""
        SELECT eom, characteristic, factor_return
        FROM contrib.global_factor_returns_monthly
        WHERE
            characteristic IN {char_list_str} AND
            excntry = 'USA'
    """

    # --- Execute the query and download the data ---
    try:
        print(f"Executing SQL query for benchmark factors: {characteristic_names}...")
        pandas_df = db.raw_sql(query, date_cols=["eom"])
        print("Query executed successfully.")
        db.close()
        print("WRDS connection closed.")
    except Exception as e:
        print(f"Failed to execute query: {e}")
        db.close()
        return

    # --- Convert to Polars and Save as Parquet ---
    try:
        polars_df = pl.from_pandas(pandas_df)
        print("Converted pandas DataFrame to Polars DataFrame.")

        output_path.parent.mkdir(parents=True, exist_ok=True)
        polars_df.write_parquet(output_path)
        print(f"Data saved successfully to {output_path}. Shape: {polars_df.shape}")
    except Exception as e:
        print(f"Failed to convert or save data: {e}")
        return

    print("--- Benchmark Data Download Complete ---")


if __name__ == "__main__":
    # This block allows the script to be run directly to download the HML benchmark.
    project_root = Path(__file__).resolve().parent.parent
    output_file_path = project_root / "data" / "raw" / "jkp_hml_benchmark_usa.parquet"
    
    # For this project, we only need the benchmark for the 'be_me' factor
    benchmarks_to_download = ["be_me"]
    
    download_jkp_benchmark_factors(
        characteristic_names=benchmarks_to_download,
        output_path=output_file_path,
    )

