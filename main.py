# main.py
"""
Main runner script for the factor replication project.
"""

from pathlib import Path
import polars as pl

from src.portfolio_construction import construct_factor_portfolio
from src.validation import validate_factor

def main():
    """Main project workflow."""
    print("--- Starting Factor Replication and Validation Workflow ---")

    project_root = Path(__file__).resolve().parent
    char_data_file = project_root / "data" / "raw" / "char" / "jkp_char_usa_202001_202510.parquet"
    replicated_output_dir = project_root / "data" / "processed"
    replicated_output_dir.mkdir(exist_ok=True)

    # --- NEW: Define a list of weighting schemes and their benchmark files ---
    schemes_to_run = [
        ('vw_cap', project_root / "data" / "raw" / "factor" / "usa_all_factors_monthly_vw_cap.csv"),
        ('ew', project_root / "data" / "raw" / "factor" / "usa_all_factors_monthly_ew.csv"),
        ('vw', project_root / "data" / "raw" / "factor" / "usa_all_factors_monthly_vw.csv"),
    ]

    factors_to_replicate = [
        ('be_me', 'be_me', 'long'),
        ('ope_be', 'ope_be', 'long'),
        ('at_gr1', 'at_gr1', 'short'),
        ('ret_12_1', 'ret_12_1', 'long'),
        ('ret_1_0', 'ret_1_0', 'short'),
        ('me', 'market_equity', 'short'),
    ]

    # --- NEW: Outer loop for each weighting scheme ---
    for scheme, benchmark_path in schemes_to_run:
        print(f"\n{'='*20} RUNNING SCHEME: {scheme.upper()} {'='*20}")

        for characteristic, factor_name, direction in factors_to_replicate:
            print(f"\n--- Processing Factor: {factor_name} ({scheme.upper()}) ---")

            # Create a unique name for the replicated factor series
            replicated_factor_name = f"{factor_name}_{scheme.upper()}"

            replicated_returns = construct_factor_portfolio(
                char_data_path=char_data_file,
                characteristic=characteristic,
                factor_name=replicated_factor_name,
                weighting_scheme=scheme, # Pass the current scheme to the function
                long_short_direction=direction
            )
            
            output_path = replicated_output_dir / f"{replicated_factor_name.lower()}_replicated.parquet"
            replicated_returns.write_parquet(output_path)
            print(f"Replicated returns saved to {output_path}")

            validate_factor(
                replicated_returns=replicated_returns,
                benchmark_path=benchmark_path,
                benchmark_factor_name=factor_name # Benchmark name in file is consistent
            )

    print("\n--- All Workflows Complete ---")

if __name__ == "__main__":
    main()