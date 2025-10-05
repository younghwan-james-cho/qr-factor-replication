# main.py
"""
Main runner script for the factor replication project.

This script orchestrates the entire workflow:
1. Defines the factors to be replicated.
2. Calls the portfolio construction module to generate factor returns.
3. Calls the validation module to compare against benchmark data.
"""

from pathlib import Path
import polars as pl

# Import our custom modules
from src.portfolio_construction import construct_factor_portfolio
from src.validation import validate_factor

def main():
    """Main project workflow."""
    print("--- Starting Factor Replication and Validation Workflow ---")

    # --- Configuration ---
    project_root = Path(__file__).resolve().parent
    char_data_file = project_root / "data" / "raw" / "char" / "jkp_char_usa_202001_202510.parquet"
    
    # Correct benchmark file for our Capped Value-Weighted replication
    benchmark_data_file = project_root / "data" / "raw" / "factor" / "usa_all_factors_monthly_vw_cap.csv"
    
    replicated_output_dir = project_root / "data" / "processed"
    replicated_output_dir.mkdir(exist_ok=True)

    # Define the factors we want to replicate
    factors_to_replicate = [
    # Characteristic, Factor Name, Direction ('short' for negative sign)
    ('be_me', 'be_me', 'long'),      # Value
    ('ope_be', 'ope_be', 'long'),    # Profitability
    ('at_gr1', 'at_gr1', 'short'),     # Investment (Note: direction is 'short')
    ('ret_12_1', 'ret_12_1', 'long'),    # Momentum
    ('ret_1_0', 'ret_1_0', 'short'),     # Short-Term Reversal (Note: direction is 'short')
]

    # --- Workflow Execution ---
    for characteristic, factor_name, direction in factors_to_replicate:
        print(f"\n--- Processing Factor: {factor_name} ---")

        # 1. Replicate the factor
        print(f"Constructing portfolio for '{characteristic}'...")
        replicated_returns = construct_factor_portfolio(
            char_data_path=char_data_file,
            characteristic=characteristic,
            factor_name=factor_name,
            long_short_direction=direction
        )
        
        output_path = replicated_output_dir / f"{factor_name.lower()}_replicated.parquet"
        replicated_returns.write_parquet(output_path)
        print(f"Replicated returns saved to {output_path}")

        # 2. Validate the factor
        print(f"Validating {factor_name} against benchmark...")
        validate_factor(
            replicated_returns=replicated_returns,
            benchmark_path=benchmark_data_file,
            benchmark_factor_name=factor_name
        )

    print("\n--- Workflow Complete ---")

if __name__ == "__main__":
    main()