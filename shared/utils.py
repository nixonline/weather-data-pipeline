from datetime import datetime, timedelta, UTC
import argparse
import json
import shutil
import os
import pandas as pd

def parse_args(description="Weather Data Pipeline"):
    """
    Parse command-line arguments for weather data pipeline.
    
    Args:
        description (str): Help text displayed when --help is used
    
    Returns:
        argparse.Namespace: Parsed arguments with attributes:
            - load_historic (bool): Download all files if True, else default range
            - start_date (str): Start date in YYYY-MM-DD format (optional)
            - end_date (str): End date in YYYY-MM-DD format (optional)
    """
    parser = argparse.ArgumentParser(description=description)

    parser.add_argument("--load-historic", action="store_true")
    parser.add_argument("--start-date", type=str)
    parser.add_argument("--end-date", type=str)

    return parser.parse_args()

def resolve_dates(load_historic, start_date, end_date):
    """
    Resolve start and end dates for data processing based on input parameters.
    
    If load_historic is False: Returns relative date range (yesterday to +7 days), ignoring start_date and end_date.
    If load_historic is True: Parses and uses provided start_date and end_date arguments with adjustments.
    
    Args:
        load_historic (bool): If True, use provided start/end dates; if False, use relative range
        start_date (str): Start date in YYYY-MM-DD format (required if load_historic=True)
        end_date (str): End date in YYYY-MM-DD format (required if load_historic=True)
    
    Returns:
        tuple: (start_date, end_date) as datetime.date objects
    
    Raises:
        ValueError: If start_date/end_date are invalid or missing when load_historic=True
    """
    today = datetime.now(UTC).date()

    base_date = today - timedelta(days=1)

    if not load_historic:
        start = base_date
        end = base_date + timedelta(days=7)  # 1 week forecast
        return start, end

    if not start_date and not end_date:
        start = base_date
        end = base_date + timedelta(days=7)
        return start, end

    # Parse inputs
    if start_date:
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        start = start - timedelta(days=1)  # adjust for lag
    else:
        raise ValueError("start_date is required if end_date is provided")

    if end_date:
        end = datetime.strptime(end_date, "%Y-%m-%d").date()
        end = end + timedelta(days=7)  # extend to forecast window
    else:
        raise ValueError("end_date is required if start_date is provided")

    if start > end:
        raise ValueError("start_date cannot be after end_date")

    return start, end

def save_run_log(saved_files, log_dir="logs"):
    """
    Save a log of the run with details about saved files.
    """
    os.makedirs(log_dir, exist_ok=True)
    
    run_ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    log_file = os.path.join(log_dir, f"extract_run_log_{run_ts}.json")

    with open(log_file, "w") as f:
        json.dump(saved_files, f, indent=2)

    return log_file

def cleanup_local_folder(folder_path):
    """
    Remove all files in a local folder and the folder itself.
    """
    if os.path.exists(folder_path):
        shutil.rmtree(folder_path)

def load_files_and_append_to_df(folder_path):
    """
    Load all CSV files from a folder and concatenate them into a single DataFrame.
    """
    all_files = []
    if not os.path.exists(folder_path):
        raise FileNotFoundError(f"Folder not found: {folder_path}")

    for file_name in os.listdir(folder_path):
        if file_name.endswith(".csv"):
            file_path = os.path.join(folder_path, file_name)
            all_files.append(file_path)

    if not all_files:
        print(f"Error: No CSV files found in {folder_path}")
        return pd.DataFrame()

    df = pd.concat((pd.read_csv(f, dtype="str") for f in all_files), ignore_index=True)
    
    return df

def apply_schema_dtypes(df, schema):
    """
    Apply dtype schema to dataframe columns.
    
    Args:
        df (pd.DataFrame): Input dataframe
        schema (dict): Dictionary mapping column name to dtype string (e.g., {"col1": "int64", "col2": "float", "col3": "datetime64[ns]"})
    
    Returns:
        pd.DataFrame: Dataframe with updated dtypes
    """
    for col, dtype in schema.items():
        if col in df.columns:
            try:
                # Special handling for datetime columns
                if "datetime" in str(dtype):
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                else:
                    df[col] = df[col].astype(dtype)
            except Exception as e:
                print(f"Warning: Could not convert column '{col}' to dtype '{dtype}': {str(e)}")
    
    return df