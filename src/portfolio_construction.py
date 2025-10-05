# src/portfolio_construction.py
"""
Functions for constructing long-short factor portfolios based on firm characteristics.
"""

import polars as pl
from pathlib import Path

def construct_factor_portfolio(
    char_data_path: Path,
    characteristic: str,
    factor_name: str,
    long_short_direction: str = 'long',
) -> pl.DataFrame:
    """
    Constructs a long-short factor portfolio based on a single characteristic.

    This function implements the methodology described in the JKP data spec:
    1. Determines size breakpoints (micro-cap, weight caps) from the universe.
    2. Determines characteristic breakpoints for terciles using non-micro stocks.
    3. Allocates all stocks to one of three portfolios (Low, Mid, High).
    4. Calculates the capped value-weighted return for each portfolio.
    5. Calculates the long-short factor return time series.

    Args:
        char_data_path (Path): Path to the Parquet file containing firm-level
                               characteristic data.
        characteristic (str): The column name of the characteristic to sort on
                              (e.g., 'be_me').
        factor_name (str): The name for the final factor return column (e.g., 'HML').
        long_short_direction (str): Determines which leg is long. 'long' means
                                    High-tercile is long; 'short' means Low-tercile
                                    is long. Defaults to 'long'.

    Returns:
        pl.DataFrame: A DataFrame with 'eom' (end-of-month date) and the
                      calculated factor return for that month.
    """
    # Step 1: Load the characteristic data from the Parquet file.
    required_cols = ['eom', 'permno', 'me', 'ret_exc_lead1m', characteristic]
    try:
        char_df = pl.read_parquet(char_data_path, columns=required_cols)
    except Exception as e:
        print(f"Error loading or finding columns in {char_data_path}: {e}")
        raise
        
    char_df = char_df.drop_nulls(subset=[characteristic, 'me'])

    # Step 2: For each month, determine the NYSE size breakpoints.
    size_breakpoints = char_df.group_by('eom').agg(
        pl.col('me').quantile(0.20).alias('me_p20'),
        pl.col('me').quantile(0.80).alias('me_p80')
    ).sort('eom')
    char_df = char_df.join(size_breakpoints, on='eom')

    # Step 3: For each month, determine characteristic tercile breakpoints.
    non_micro_caps = char_df.filter(pl.col('me') > pl.col('me_p20'))
    char_breakpoints = non_micro_caps.group_by('eom').agg(
        pl.col(characteristic).quantile(1/3).alias('char_p33'),
        pl.col(characteristic).quantile(2/3).alias('char_p67')
    ).sort('eom')
    char_df = char_df.join(char_breakpoints, on='eom')
    
    # Step 4: Allocate ALL stocks to portfolios.
    char_df = char_df.with_columns(
        pl.when(pl.col(characteristic) <= pl.col('char_p33'))
        .then(pl.lit('Low'))
        .when(pl.col(characteristic) > pl.col('char_p67'))
        .then(pl.lit('High'))
        .otherwise(pl.lit('Mid'))
        .alias('portfolio')
    )

    # Step 5: Calculate capped value-weighted returns for each portfolio.
    portfolio_returns = char_df.group_by(['eom', 'portfolio']).agg(
        (
            (
                pl.when(pl.col('me') > pl.col('me_p80'))
                .then(pl.col('me_p80'))
                .otherwise(pl.col('me'))
                .alias('capped_me')
                / 
                pl.when(pl.col('me') > pl.col('me_p80'))
                .then(pl.col('me_p80'))
                .otherwise(pl.col('me'))
                .sum()
            ) * pl.col('ret_exc_lead1m')
        ).sum().alias('port_ret')
    ).sort('eom')
    
    # Step 6: Calculate the long-short factor return and format the output.
    portfolio_returns_wide = portfolio_returns.pivot(
        index='eom',
        columns='portfolio',
        values='port_ret'
    ).sort('eom')

    if long_short_direction == 'long':
        factor_returns = portfolio_returns_wide.with_columns(
            (pl.col('High') - pl.col('Low')).alias(factor_name)
        )
    else: # 'short'
        factor_returns = portfolio_returns_wide.with_columns(
            (pl.col('Low') - pl.col('High')).alias(factor_name)
        )
    
    return factor_returns.select(['eom', factor_name])


if __name__ == '__main__':
    project_root = Path(__file__).resolve().parent.parent
    char_data_file = project_root / "data" / "raw" / "char" / "jkp_char_usa_202001_202510.parquet"

    # Test the function for the Value factor ('be_me' -> HML)
    value_factor_returns = construct_factor_portfolio(
        char_data_path=char_data_file,
        characteristic='be_me',
        factor_name='HML'
    )
    print("Successfully calculated Value (HML) factor returns:")
    print(value_factor_returns.head())