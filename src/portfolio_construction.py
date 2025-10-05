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
    weighting_scheme: str = 'vw_cap',
    long_short_direction: str = 'long',
) -> pl.DataFrame:
    """
    Constructs a long-short factor portfolio based on a single characteristic,
    supporting multiple weighting schemes.
    """
    # --- FIX: Define 'base_cols' before using it ---
    base_cols = ['eom', 'permno', 'crsp_exchcd', 'me', 'ret_exc_lead1m']
    all_cols = base_cols + [characteristic]
    required_cols = list(dict.fromkeys(all_cols)) # Removes duplicates
    # --- End of FIX ---
    
    char_df = pl.read_parquet(char_data_path, columns=required_cols).drop_nulls(subset=[characteristic, 'me'])

    if characteristic == 'be_me':
        char_df = char_df.filter(pl.col(characteristic) > 0)

    nyse_stocks = char_df.filter(pl.col('crsp_exchcd') == 1)
    
    size_breakpoints = nyse_stocks.group_by('eom').agg(
        pl.col('me').quantile(0.20).alias('me_p20'),
        pl.col('me').quantile(0.80).alias('me_p80')
    ).sort('eom')

    char_df = char_df.join(size_breakpoints, on='eom')

    non_micro_caps = char_df.filter(pl.col('me') > pl.col('me_p20'))
    char_breakpoints = non_micro_caps.group_by('eom').agg(
        pl.col(characteristic).quantile(1/3).alias('char_p33'),
        pl.col(characteristic).quantile(2/3).alias('char_p67')
    ).sort('eom')
    char_df = char_df.join(char_breakpoints, on='eom')
    
    char_df = char_df.with_columns(
        pl.when(pl.col(characteristic) <= pl.col('char_p33'))
        .then(pl.lit('Low'))
        .when(pl.col(characteristic) > pl.col('char_p67'))
        .then(pl.lit('High'))
        .otherwise(pl.lit('Mid'))
        .alias('portfolio')
    )

    if weighting_scheme == 'ew':
        weighting_expr = pl.mean('ret_exc_lead1m').alias('port_ret')
    elif weighting_scheme == 'vw':
        weighting_expr = ((pl.col('me') / pl.col('me').sum()) * pl.col('ret_exc_lead1m')).sum().alias('port_ret')
    elif weighting_scheme == 'vw_cap':
        weighting_expr = ((
            pl.when(pl.col('me') > pl.col('me_p80')).then(pl.col('me_p80')).otherwise(pl.col('me')) /
            pl.when(pl.col('me') > pl.col('me_p80')).then(pl.col('me_p80')).otherwise(pl.col('me')).sum()
        ) * pl.col('ret_exc_lead1m')).sum().alias('port_ret')
    else:
        raise ValueError(f"Unknown weighting scheme: {weighting_scheme}")

    portfolio_returns = char_df.group_by(['eom', 'portfolio']).agg(
        weighting_expr,
        pl.len().alias('n_stocks')
    ).sort('eom')
    
    portfolio_returns_wide = portfolio_returns.pivot(
        index='eom', columns='portfolio', values=['port_ret', 'n_stocks']
    ).sort('eom').filter(
        (pl.col('n_stocks_Low') >= 5) & (pl.col('n_stocks_High') >= 5)
    )

    if long_short_direction == 'long':
        factor_returns = portfolio_returns_wide.with_columns(
            (pl.col('port_ret_High') - pl.col('port_ret_Low')).alias(factor_name)
        )
    else:
        factor_returns = portfolio_returns_wide.with_columns(
            (pl.col('port_ret_Low') - pl.col('port_ret_High')).alias(factor_name)
        )
    
    return factor_returns.select(['eom', factor_name])