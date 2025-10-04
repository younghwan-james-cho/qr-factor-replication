# src/data_ingestion.py
"""
Functions for downloading firm-level characteristics for factor construction
from the Wharton Research Data Services (WRDS) JKP database.
"""

import getpass
import wrds
import polars as pl
from pathlib import Path
from typing import List, Optional

# Import the username from our secure configuration file
try:
    from config.wrds_config import WRDS_USERNAME
except ImportError:
    WRDS_USERNAME = None


def download_jkp_char_data(
    characteristics: List[str],
    output_path: Path,
    wrds_username: str = WRDS_USERNAME,
    start_date: Optional[str] = None,
) -> None:
    """
    Connects to WRDS and downloads specified firm characteristics data for US stocks
    from the JKP global factor database ('contrib.global_factor').
    """
    print("--- Starting JKP Characteristic Data Download ---")

    if not wrds_username or wrds_username == "your_username_here":
        print("Error: WRDS_USERNAME not set in config/wrds_config.py. Please update it.")
        return

    # --- Securely connect to WRDS ---
    try:
        print(f"Connecting to WRDS with username: {wrds_username}...")
        # .pgpass 파일을 사용하여 비밀번호 없이 연결
        db = wrds.Connection(wrds_username=wrds_username)
        print("Successfully connected to WRDS.")
    except Exception as e:
        print(f"Failed to connect to WRDS: {e}")
        return

    # --- Define the SQL Query ---
    core_cols = ["eom", "id", "permno", "size_grp", "me", "ret_exc_lead1m"]
    unique_chars = [char for char in characteristics if char not in core_cols]
    all_cols = core_cols + unique_chars
    columns_str = ", ".join(all_cols)

    date_filter = f"AND eom >= '{start_date}'" if start_date else ""

    query = f"""
        SELECT {columns_str}
        FROM contrib.global_factor
        WHERE
            excntry = 'USA' AND
            common = 1 AND
            exch_main = 1 AND
            primary_sec = 1 AND
            obs_main = 1
            {date_filter}
    """

    # --- Execute the query and download the data ---
    try:
        print(f"Executing SQL query for characteristics: {all_cols}...")
        # wrds 라이브러리는 pandas DataFrame을 반환하며, 이를 처리합니다.
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

    print("--- Characteristic Data Download Complete ---")


if __name__ == "__main__":
    import datetime
    import pandas as pd  # 수정: 누락된 pandas 임포트 추가

    # This block allows the script to be run directly.
    project_root = Path(__file__).resolve().parent.parent
    
    start_date_str = '2020-01-01'
    # 날짜 범위를 위해 동적으로 오늘 날짜 가져오기
    end_date_str = datetime.date.today().strftime('%Y-%m-%d')
    
    # 파일명을 위한 날짜 포맷팅 (YYYYMM)
    start_fname = pd.to_datetime(start_date_str).strftime('%Y%m')
    end_fname = pd.to_datetime(end_date_str).strftime('%Y%m')

    # 표준 규칙에 따른 출력 파일명 정의
    output_filename = f"jkp_char_usa_{start_fname}_{end_fname}.parquet"
    output_file_path = project_root / "data" / "raw" / "char" / output_filename

    # 다운로드할 특성 변수 정의
    characteristics_to_download = [
        "be_me",    # Value (HML)
        "ope_be",   # Profitability (RMW)
        "at_gr1",   # Investment (CMA)
        "ret_12_1", # Momentum (MOM)
        "ret_1_0",  # Short-Term Reversal (STR)
    ]

    # 다운로드 함수 호출
    download_jkp_char_data(
        characteristics=characteristics_to_download,
        output_path=output_file_path,
        start_date=start_date_str,
    )