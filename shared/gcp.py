from datetime import datetime, timedelta, UTC
from google.cloud import storage
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
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)
    blob.upload_from_filename(source_file)

def upload_files_to_gcs(saved_files, bucket_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    today = datetime.now(UTC).date()

    for file in saved_files:
        if file["status"] != "saved":
            continue

        file_date_str = file["file_name"].split("_")[1].replace(".csv", "")
        file_date = datetime.strptime(file_date_str, "%Y-%m-%d").date()

        gcs_path = f"weather/{file['file_name']}"

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

def upload_log_to_gcs(log_file, bucket_name):
    file_name = os.path.basename(log_file)
    gcs_path = f"logs/{file_name}"
    upload_to_gcs(bucket_name, log_file, gcs_path)

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
        
        files_to_download = matching_files if load_historic else [matching_files[0]]
        
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
