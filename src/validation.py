# src/validation.py
#
# Functions for validating replicated factor returns.
#

from pathlib import Path
import polars as pl
import numpy as np
import matplotlib.pyplot as plt

def plot_cumulative_returns(df: pl.DataFrame, factor_name: str, plot_subdir: str):
    """
    Generates and saves a plot of cumulative factor returns into a specific subdirectory.
    """
    plot_df = df.to_pandas()
    
    plot_df['replicated_cumulative'] = (1 + plot_df[factor_name]).cumprod()
    plot_df['benchmark_cumulative'] = (1 + plot_df['benchmark_ret']).cumprod()

    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(12, 7))

    ax.plot(plot_df['eom'], plot_df['replicated_cumulative'], label='Replicated Factor', linewidth=2)
    ax.plot(plot_df['eom'], plot_df['benchmark_cumulative'], label='Benchmark Factor', linestyle='--', linewidth=2)
    
    ax.set_title(f'Cumulative Performance: {factor_name}', fontsize=16)
    ax.set_xlabel('Date', fontsize=12)
    ax.set_ylabel('Cumulative Return', fontsize=12)
    ax.legend(fontsize=12)
    ax.set_yscale('log')
    
    fig.tight_layout()
    
    output_dir = Path("plots") / plot_subdir
    output_dir.mkdir(parents=True, exist_ok=True)
    
    plot_filename = f"{factor_name.lower()}.png"
    plot_path = output_dir / plot_filename

    fig.savefig(plot_path)
    print(f"Performance plot saved to: {plot_path}")
    plt.close(fig)

def validate_factor(
    replicated_returns: pl.DataFrame,
    benchmark_path: Path,
    benchmark_factor_name: str,
    plot_subdir: str,
    correlation_threshold: float = 0.95
) -> float | None:
    """
    Validates a replicated factor against its benchmark.
    Returns the correlation value if successful, otherwise None.
    """
    try:
        benchmark_df = pl.read_csv(benchmark_path, try_parse_dates=True)
        benchmark_factor = benchmark_df.filter(
            pl.col('name') == benchmark_factor_name
        ).select(['date', 'ret']).rename({'ret': 'benchmark_ret', 'date': 'eom'})
    except Exception as e:
        print(f"Error loading or processing benchmark data: {e}")
        return None

    # Align dates for comparison.
    replicated_returns = replicated_returns.with_columns(pl.col('eom').dt.offset_by('1mo'))
    replicated_returns = replicated_returns.with_columns(pl.col('eom').cast(pl.Date))
    benchmark_factor = benchmark_factor.with_columns(pl.col('eom').cast(pl.Date))

    replicated_col_name = replicated_returns.columns[1]
    validation_df = replicated_returns.join(benchmark_factor, on='eom', how='inner').drop_nulls()

    if validation_df.height == 0:
        print("Validation failed: No overlapping dates found.")
        return None

    correlation = validation_df.select(pl.corr(replicated_col_name, 'benchmark_ret')).item()
    
    print("\n" + "="*40)
    print(f"      VALIDATION REPORT: {benchmark_factor_name}")
    print("="*40)
    print(f"Replication Success Metric (> {correlation_threshold:.2f} Correlation)")
    print(f"  - Time-Series Correlation: {correlation:.4f}")
    if correlation > correlation_threshold:
        print("  - Status: SUCCESS")
    else:
        print("  - Status: FAILURE")
    print("-"*40)
    
    summary_stats = validation_df.select(
        pl.mean(replicated_col_name).alias('Mean (Replicated)'),
        pl.mean('benchmark_ret').alias('Mean (Benchmark)'),
        (pl.std(replicated_col_name) * np.sqrt(12)).alias('Ann. Vol (Replicated)'),
        (pl.std('benchmark_ret') * np.sqrt(12)).alias('Ann. Vol (Benchmark)'),
    )
    print("Summary Statistics:")
    print(summary_stats.transpose(include_header=True, column_names=['Metric']))
    print("="*40 + "\n")

    plot_cumulative_returns(validation_df, replicated_col_name, plot_subdir)
    
    # Return the calculated correlation for the summary table.
    return correlation