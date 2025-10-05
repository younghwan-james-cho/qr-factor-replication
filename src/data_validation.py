# src/data_validation.py
#
# Functions for validating the raw input data.
#

import polars as pl

def validate_raw_data(df: pl.DataFrame) -> None:
    """
    Performs a series of validation checks on the raw characteristic data.
    Raises a ValueError for critical failures but only warns for non-critical issues.
    
    Args:
        df (pl.DataFrame): The raw data to validate.
    """
    print("--- Running Raw Data Validation Checks ---")
        
    # Check 1: Critical identifiers. These MUST NOT be null.
    critical_id_cols = ['eom', 'id']
    id_null_counts = df.select(pl.col(critical_id_cols).is_null().sum()).row(0)
    for i, col in enumerate(critical_id_cols):
        if id_null_counts[i] > 0:
            raise ValueError(f"CRITICAL Validation failed: Column '{col}' contains {id_null_counts[i]} null values.")

    # Check 2: Important data columns. Nulls are undesirable but can be cleaned
    data_cols_to_check = ['me', 'ret_exc_lead1m']
    data_null_counts = df.select(pl.col(data_cols_to_check).is_null().sum()).row(0)
    for i, col in enumerate(data_cols_to_check):
        if data_null_counts[i] > 0:
            print(f"Warning: Column '{col}' contains {data_null_counts[i]} null values. These will be dropped in the portfolio construction step.")
            
    # Check 3: Check for unrealistic return values.
    max_ret = df.select(pl.col('ret_exc_lead1m').abs().max()).item()
    if max_ret > 10.0:
        print(f"Warning: Maximum absolute monthly return is {(max_ret*100):.2f}%, which is unusually high.")

    print("Raw data validation checks complete.")