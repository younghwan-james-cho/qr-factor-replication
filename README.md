Project 1: Academic Factor Replication
Objective
The objective of this project is to replicate five standard academic investment factors using the firm-level characteristic data from the Jensen, Kelly, and Pedersen (JKP) global dataset.

The replicated factor returns are then validated against the pre-computed benchmark factors provided by the JKP authors to test the accuracy of the replication.

Project Workflow
This project follows a professional quantitative research workflow, separating data ingestion, portfolio construction, and validation into modular scripts.

Data Ingestion: Download the necessary raw firm-level data (characteristics) from the Wharton Research Data Services (WRDS) database.

Factor Replication: Use the characteristic data to construct long-short factor portfolios based on academic methodologies. The output is a time series of returns for each replicated factor.

Validation: Compare the replicated factor returns against the official JKP benchmark returns. The primary metric for a successful replication is a time-series correlation greater than 0.95.

Data Sources
This project uses two distinct datasets:

Firm-Level Characteristic Data

Source: WRDS contrib.global_factor table.

Purpose: The raw ingredients used to build our own factor portfolios.

Key Columns: be_me (Value), ope_be (Profitability), at_gr1 (Investment), ret_12_1 (Momentum), ret_1_0 (Short-Term Reversal), and me (Market Equity for weighting).

JKP Benchmark Factor Data

Source: CSV files downloaded from jkpfactors.com.

Purpose: The "ground truth" or benchmark against which we validate our replicated factors.

Location: Stored in the data/raw/factor/ directory.

Setup Instructions
This project uses a Conda environment for reproducibility.

Create and activate the Conda environment:

conda create --name qr_env python=3.11
conda activate qr_env

Install required packages: We prioritize the conda-forge channel for stability.

# Install connectorx for high-performance database access
conda install -c conda-forge connectorx

# Install main data and plotting libraries
conda install -c conda-forge polars pandas plotly numpy jupyterlab

# Install the WRDS library
pip install wrds

Configure WRDS Access:

Copy the configuration template: cp config/wrds_config.py.template config/wrds_config.py

Edit config/wrds_config.py and replace the placeholder with your actual WRDS username.

How to Run the Analysis
The entire workflow for Project 1 can be executed with two commands from the terminal.

Step 1: Download Firm-Level Data
Run the data_ingestion.py script to download the characteristic data from WRDS. You can specify a start date for your analysis period.

Example (for data from 2010 onwards):

python -m src.data_ingestion --start_date 2010-01-01

Example (for data from 2020 onwards):

python -m src.data_ingestion --start_date 2020-01-01

Step 2: Run Replication and Validation
Run the run_analysis.py script. This will first construct the factor portfolios from the data you just downloaded and then automatically validate them against the JKP benchmark files. You can specify the portfolio weighting scheme (vw or ew).

To run value-weighted replication and validation:

python run_analysis.py --weighting vw

To run equal-weighted replication and validation:

python run_analysis.py --weighting ew

The script will print the validation results, including the correlation score, for each factor and display the cumulative return plots.