# Project 1: Academic Factor Replication
## Objective
The objective of this project is to replicate five standard academic investment factors using the firm-level characteristic data from the Jensen, Kelly, and Pedersen (JKP) global dataset.

The replicated factor returns are then validated against the pre-computed benchmark factors provided by the JKP authors to test the accuracy of the replication.

## Project Workflow
This project follows a professional quantitative research workflow, separating data ingestion, portfolio construction, and validation into modular scripts.

1. Data Ingestion: Download the necessary raw firm-level data (characteristics) from the Wharton Research Data Services (WRDS) database.

2. Factor Replication: Use the characteristic data to construct long-short factor portfolios based on academic methodologies. The output is a time series of returns for each replicated factor.

3. Validation: Compare the replicated factor returns against the official JKP benchmark returns. The primary metric for a successful replication is a time-series correlation greater than 0.95.

## Data Sources
This project uses two distinct datasets:

1. Firm-Level Characteristic Data
- Source: WRDS contrib.global_factor table.
- Purpose: The raw ingredients used to build our own factor portfolios.
- Key Columns: be_me (Value), ope_be (Profitability), at_gr1 (Investment), ret_12_1 (Momentum), ret_1_0 (Short-Term Reversal), and me (Market Equity for weighting).

2. JKP Benchmark Factor Data
- Source: CSV files downloaded from jkpfactors.com.
- Purpose: The "ground truth" or benchmark against which we validate our replicated factors.
- Location: Stored in the data/raw/factor/ directory.