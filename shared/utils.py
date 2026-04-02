from datetime import datetime, timedelta, UTC
import argparse
import json
import shutil
import os

def parse_args():
    parser = argparse.ArgumentParser(description="Weather Data Extractor")

    parser.add_argument("--load-historic", action="store_true")
    parser.add_argument("--start-date", type=str)
    parser.add_argument("--end-date", type=str)

    return parser.parse_args()

def resolve_dates(load_historic, start_date, end_date):
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
    os.makedirs(log_dir, exist_ok=True)
    
    run_ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    log_file = os.path.join(log_dir, f"extract_run_log_{run_ts}.json")

    with open(log_file, "w") as f:
        json.dump(saved_files, f, indent=2)

    return log_file

def cleanup_local_folder(folder_path="output"):
    if os.path.exists(folder_path):
        shutil.rmtree(folder_path)
