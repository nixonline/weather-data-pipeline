from datetime import datetime, timedelta, UTC
from google.cloud import storage, bigquery
from google.oauth2 import service_account
import os
import json

def get_gcs_client():
    service_key_str = os.getenv("GCP_SERVICE_KEY")
    if not service_key_str:
        raise ValueError("GCP_SERVICE_KEY environment variable not set")

    service_key_dict = json.loads(service_key_str)
    credentials = service_account.Credentials.from_service_account_info(service_key_dict)
    client = storage.Client(credentials=credentials, project=service_key_dict["project_id"])
    return client

def upload_to_gcs(bucket_name, source_file, destination_blob):
    """
    Uploads a file to Google Cloud Storage.
    """
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)
    blob.upload_from_filename(source_file)

def upload_files_to_gcs(bucket_name, destination_blob_prefix, saved_files):
    """
    Uploads a list of files to Google Cloud Storage.
    
    Args:
        bucket_name (str): GCS bucket name
        destination_blob_prefix (str): GCS path prefix (e.g., "weather", "logs")
        saved_files (list): List of dicts with "file_name" and "file_path" keys
    
    Returns:
        list: Updated saved_files list with upload status
    """
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    today = datetime.now(UTC).date()

    for file in saved_files:
        if file["status"] != "saved":
            continue

        file_date_str = file["file_name"].split("_")[1].replace(".csv", "")
        file_date = datetime.strptime(file_date_str, "%Y-%m-%d").date()

        gcs_path = f"{destination_blob_prefix}/{file['file_name']}"

        if file_date < today:
            blob = bucket.blob(gcs_path)
            if blob.exists():
                file["status"] = "skipped"
                continue

        try:
            blob = bucket.blob(gcs_path)
            blob.upload_from_filename(file["file_path"])
            file["status"] = "uploaded"

        except Exception as e:
            file["status"] = "upload_failed"
            file["error"] = str(e)

    return saved_files

def download_weather_data(bucket_name, blob_prefix, local_dir, load_historic=False):
    """
    Download weather data files from GCS blob storage.
    
    Args:
        bucket_name: GCS bucket name (e.g., "weather-data-1001")
        blob_prefix: Path prefix in bucket (e.g., "daily")
        local_dir: Local directory to save files
        load_historic: If True, download all files. If False, download only the latest file.
    
    Returns:
        dict with "status", "files_downloaded", and "error" (if any)
    """
    try:
        client = get_gcs_client()
        bucket = client.bucket(bucket_name)
        
        # Create local directory if it doesn't exist
        os.makedirs(local_dir, exist_ok=True)
        
        # List all blobs with the given prefix
        blobs = list(bucket.list_blobs(prefix=blob_prefix))
        
        # Filter for files matching the pattern weather_batch_YYYYMMDD_TS.csv
        matching_files = []
        for blob in blobs:
            file_name = blob.name.split('/')[-1]  # Get just the filename
            # Check if file matches the pattern weather_batch_YYYYMMDD_*.csv
            if file_name.startswith("weather_batch_") and file_name.endswith(".csv"):
                matching_files.append(blob)
        
        if not matching_files:
            return {
                "status": "no_files_found",
                "files_downloaded": [],
                "message": f"No matching files found in {bucket_name}/{blob_prefix}"
            }
        
        # Sort by blob name to find the latest (most recent date/timestamp)
        matching_files.sort(key=lambda b: b.name, reverse=True)
        
        files_to_download = matching_files if load_historic else matching_files[:7]
        
        downloaded_files = []
        for blob in files_to_download:
            file_name = blob.name.split('/')[-1]
            local_file_path = os.path.join(local_dir, file_name)
            blob.download_to_filename(local_file_path)
            downloaded_files.append({
                "file_name": file_name,
                "local_path": local_file_path,
                "gcs_path": blob.name
            })
        
        return {
            "status": "success",
            "files_downloaded": downloaded_files,
            "count": len(downloaded_files)
        }
        
    except Exception as e:
        return {
            "status": "failed",
            "files_downloaded": [],
            "error": str(e)
        }

def upsert_transformed_weather_to_bq(df, table_id):
    """
    Upsert transformed weather data to BigQuery table.
    Creates table on first run. On subsequent runs, deletes rows with dates >= min date in df,
    then appends new data (ensuring forecasts are always up-to-date).
    
    Args:
        df: pandas DataFrame with transformed weather data
        table_id: BigQuery table ID (e.g., "project.dataset.table")
    
    Returns:
        dict with "status", "rows_inserted", "rows_deleted", and "table_id" or "error"
    """
    try:
        if df.empty:
            return {
                "status": "no_data",
                "rows_inserted": 0,
                "rows_deleted": 0,
                "table_id": table_id
            }

        service_key_str = os.getenv("GCP_SERVICE_KEY")
        if not service_key_str:
            raise ValueError("GCP_SERVICE_KEY environment variable not set")
        
        service_key_dict = json.loads(service_key_str)
        credentials = service_account.Credentials.from_service_account_info(service_key_dict)
        client = bigquery.Client(credentials=credentials, project=service_key_dict["project_id"])
        
        # Check if table exists
        rows_deleted = 0
        try:
            client.get_table(table_id)
            table_exists = True
        except Exception:
            table_exists = False
        
        if table_exists:
            # Get minimum date from incoming DataFrame
            min_date = df["date"].min()
            
            # Delete rows with date >= min_date to remove stale forecasts
            delete_query = f"""
            DELETE FROM `{table_id}`
            WHERE date >= '{min_date}'
            """
            delete_job = client.query(delete_query)
            delete_job.result()
            rows_deleted = delete_job.num_dml_affected_rows or 0
        
        # Append new data
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        
        return {
            "status": "success",
            "rows_inserted": len(df),
            "rows_deleted": rows_deleted,
            "table_id": table_id
        }
    
    except Exception as e:
        return {
            "status": "failed",
            "rows_inserted": 0,
            "rows_deleted": 0,
            "error": str(e)
        }

def upsert_run_logs_to_bq(log_data, table_id):
    """
    Upsert run logs to BigQuery logs table.
    Appends log records (no deletion - logs are immutable historical records).
    
    Args:
        log_data: list of dicts or DataFrame containing log records
        table_id: BigQuery table ID (e.g., "project.dataset.logs_table")
    
    Returns:
        dict with "status", "rows_inserted", and "table_id" or "error"
    """
    try:
        service_key_str = os.getenv("GCP_SERVICE_KEY")
        if not service_key_str:
            raise ValueError("GCP_SERVICE_KEY environment variable not set")
        
        service_key_dict = json.loads(service_key_str)
        credentials = service_account.Credentials.from_service_account_info(service_key_dict)
        client = bigquery.Client(credentials=credentials, project=service_key_dict["project_id"])
        
        # Convert list of dicts to DataFrame if needed
        if isinstance(log_data, list):
            import pandas as pd
            df_logs = pd.DataFrame(log_data)
        else:
            df_logs = log_data
        
        if df_logs.empty:
            return {
                "status": "no_data",
                "rows_inserted": 0,
                "table_id": table_id
            }
        
        # Add timestamp if not already present
        if "log_timestamp" not in df_logs.columns:
            df_logs["log_timestamp"] = datetime.now(UTC).isoformat()
        
        # Append logs (logs are immutable, never delete)
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        
        job = client.load_table_from_dataframe(df_logs, table_id, job_config=job_config)
        job.result()
        
        return {
            "status": "success",
            "rows_inserted": len(df_logs),
            "table_id": table_id
        }
    
    except Exception as e:
        return {
            "status": "failed",
            "rows_inserted": 0,
            "error": str(e)
        }
