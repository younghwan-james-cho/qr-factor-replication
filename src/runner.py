# src/runner.py
#
# Core workflow orchestration functions for the project.
#

import datetime
from pathlib import Path
import polars as pl

# Import project modules and central settings
from config import settings
from src.data_ingestion import download_jkp_char_data
from src.data_validation import validate_raw_data
from src.portfolio_construction import construct_factor_portfolio
from src.validation import validate_factor

def get_data_filepath() -> Path:
    """
    Constructs the filepath for the raw characteristic data parquet file
    based on the start date in the settings.
    """
    start_fname = datetime.datetime.strptime(settings.START_DATE_STR, '%Y-%m-%d').strftime('%Y%m')
    end_fname = datetime.date.today().strftime('%Y%m')
    char_data_filename = f"jkp_char_usa_{start_fname}_{end_fname}.parquet"
    return settings.PROJECT_ROOT / "data" / "raw" / "char" / char_data_filename

def run_data_ingestion():
    """Runs the full data ingestion and validation pipeline."""
    char_data_file = get_data_filepath()
    
    characteristics_to_download = list(set(char for char, _, _ in settings.FACTORS_TO_REPLICATE))
    
    download_jkp_char_data(
        characteristics=characteristics_to_download,
        output_path=char_data_file,
        start_date=settings.START_DATE_STR,
    )
    
    raw_df = pl.read_parquet(char_data_file)
    validate_raw_data(raw_df)

def print_summary_table(results: list):
    """
    Prints a final summary table of all replication correlations.
    
    Args:
        results (list): A list of dictionaries, each containing the results
                        of a single validation run.
    """
    print("\n" + "="*50)
    print(" " * 12 + "Final Replication Summary")
    print("="*50)
    
    # Create a polars DataFrame from the list of result dictionaries.
    results_df = pl.DataFrame(results)
    
    # Pivot the DataFrame to get the desired table structure.
    summary_table = results_df.pivot(
        index='Factor',
        columns='Scheme',
        values='Correlation'
    )
    
    # Ensure columns are in a consistent order.
    column_order = ['Factor', 'ew', 'vw', 'vw_cap']
    existing_columns = [col for col in column_order if col in summary_table.columns]
    
    print(summary_table.select(existing_columns))
    print("="*50)

def run_replication_workflow():
    """Runs the full factor replication and validation workflow."""
    print("--- Starting Full Factor Replication and Validation Workflow ---")
    
    char_data_file = get_data_filepath()
    replicated_output_dir = settings.PROJECT_ROOT / "data" / "processed"
    replicated_output_dir.mkdir(parents=True, exist_ok=True)
    
    start_fname = datetime.datetime.strptime(settings.START_DATE_STR, '%Y-%m-%d').strftime('%Y%m')
    end_fname = datetime.date.today().strftime('%Y%m')
    plot_subdir = f"{start_fname}-{end_fname}"

    # Create a list to store results for the final summary.
    all_results = []

    for scheme, benchmark_path in settings.SCHEMES.items():
        print(f"\n{'='*20} RUNNING SCHEME: {scheme.upper()} {'='*20}")

        for characteristic, factor_name, direction in settings.FACTORS_TO_REPLICATE:
            print(f"\n--- Processing Factor: {factor_name} ({scheme.upper()}) ---")

            replicated_factor_name = f"{factor_name}_{scheme.upper()}"

            replicated_returns = construct_factor_portfolio(
                char_data_path=char_data_file,
                characteristic=characteristic,
                factor_name=replicated_factor_name,
                weighting_scheme=scheme,
                long_short_direction=direction
            )
            
            output_path = replicated_output_dir / f"{replicated_factor_name.lower()}_replicated.parquet"
            replicated_returns.write_parquet(output_path)
            print(f"Replicated returns saved to {output_path}")

            # The validation function now returns the correlation value.
            correlation = validate_factor(
                replicated_returns=replicated_returns,
                benchmark_path=benchmark_path,
                benchmark_factor_name=factor_name,
                plot_subdir=plot_subdir,
                correlation_threshold=settings.CORRELATION_THRESHOLD
            )
            
            # Append the result to our summary list.
            if correlation is not None:
                all_results.append({
                    "Factor": factor_name,
                    "Scheme": scheme,
                    "Correlation": correlation
                })

    # Print the final summary table at the very end.
    if all_results:
        print_summary_table(all_results)
        
    print("\n--- All Workflows Complete ---")