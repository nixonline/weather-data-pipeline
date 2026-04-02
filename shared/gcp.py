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
