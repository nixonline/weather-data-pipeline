import os
import sys
import json
import time
import yaml
import logging
import requests
import pandas as pd
from datetime import datetime, UTC
from dotenv import load_dotenv
from google.cloud import storage
from google.oauth2 import service_account
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Resolve imports from the project root so shared modules are loaded consistently.
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SCRIPT_DIR)
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from shared.gcp import upload_log_to_gcs
from shared.utils import parse_args, resolve_dates, save_run_log, cleanup_local_folder


class WeatherExtractor:
    def __init__(self, config_file="config.yaml"):
        self.script_dir = SCRIPT_DIR
        self.root_dir = ROOT_DIR
        config_path = os.path.join(self.script_dir, config_file)
        env_path = os.path.join(self.root_dir, ".env")

        logging.basicConfig(
            level=logging.INFO, 
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

        if os.path.exists(env_path):
            load_dotenv(env_path)
            self.logger.info(f"Loaded .env from root: {env_path}")
        else:
            self.logger.info("No .env file found in root; using system environment variables.")

        self.bucket_name = os.getenv("GCS_BUCKET")
        service_key_str = os.getenv("GCP_SERVICE_KEY")

        if not self.bucket_name:
            self.logger.error("GCS_BUCKET not found. Check your .env or Kestra env vars.")
            raise ValueError("GCS_BUCKET environment variable not set")

        if not service_key_str:
            self.logger.error("GCP_SERVICE_KEY not found. Check your .env or Kestra env vars.")
            raise ValueError("GCP_SERVICE_KEY environment variable not set")

        try:
            service_key_dict = json.loads(service_key_str.strip())
            credentials = service_account.Credentials.from_service_account_info(service_key_dict)
            self.gcs_client = storage.Client(
                credentials=credentials, 
                project=service_key_dict.get("project_id")
            )
            self.logger.info("GCS Client initialized successfully.")
        except Exception as e:
            self.logger.error(f"Failed to initialize GCS: {e}")
            raise

        if not os.path.exists(config_path):
            self.logger.error(f"config.yaml missing in extractor folder: {config_path}")
            raise FileNotFoundError(f"Ensure config.yaml is moved to {self.script_dir}")

        try:
            with open(config_path, "r") as f:
                config = yaml.safe_load(f)
            config = config or {}
            self.cities = config.get("cities", {})
            request_settings = config.get("request_settings", {})
            self.logger.info(f"Loaded {len(self.cities)} cities from config.")
        except Exception as e:
            self.logger.error(f"Error parsing config.yaml: {e}")
            raise

        self.url = "https://api.open-meteo.com/v1/forecast"
        self.request_timeout = (
            request_settings.get("connect_timeout_seconds", 10),
            request_settings.get("read_timeout_seconds", 30),
        )
        self.request_delay_seconds = request_settings.get("delay_between_requests_seconds", 0.3)
        self.request_retry_total = request_settings.get("retry_total", 3)
        self.request_retry_backoff_factor = request_settings.get("retry_backoff_factor", 1)
        self.request_retry_statuses = request_settings.get(
            "retry_statuses",
            [429, 500, 502, 503, 504],
        )
        self.session = self._build_http_session()

    def _build_http_session(self):
        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": "weather-data-pipeline/1.0",
                "Accept": "application/json",
            }
        )
        retry = Retry(
            total=self.request_retry_total,
            backoff_factor=self.request_retry_backoff_factor,
            status_forcelist=self.request_retry_statuses,
            allowed_methods=["GET"],
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def extract(self, start_date=None, end_date=None, load_historic=False):
        start_date, end_date = resolve_dates(load_historic, start_date, end_date)
        self.logger.info(f"Retrieving weather data from {start_date} to {end_date}")

        results = {}
        for index, (city, info) in enumerate(self.cities.items()):
            params = {
                "latitude": info["lat"],
                "longitude": info["lon"],
                "daily": ",".join([
                    "temperature_2m_max",
                    "temperature_2m_min",
                    "precipitation_sum",
                    "precipitation_probability_max",
                    "weathercode"
                ]),
                "timezone": info["tz"],
                "start_date": start_date,
                "end_date": end_date
            }

            try:
                response = self.session.get(
                    self.url,
                    params=params,
                    timeout=self.request_timeout,
                )
                response.raise_for_status()
                data = response.json()
                if "daily" in data:
                    results[city] = {"country": info["country"], "daily": data["daily"]}
            except Exception as e:
                self.logger.error(f"Error fetching {city}: {e}")

            if index < len(self.cities) - 1:
                time.sleep(self.request_delay_seconds)
        return results

    def parse_to_dataframe(self, results):
        records = []
        extraction_timestamp = datetime.now(UTC).isoformat()

        for city, info in results.items():
            daily = info["daily"]
            for i in range(len(daily["time"])):
                records.append({
                    "city": city,
                    "country": info["country"],
                    "date": daily["time"][i],
                    "temp_max": daily["temperature_2m_max"][i],
                    "temp_min": daily["temperature_2m_min"][i],
                    "precip_sum": daily["precipitation_sum"][i],
                    "precip_prob_max": daily["precipitation_probability_max"][i],
                    "weather_code": daily["weathercode"][i],
                    "extracted_at": extraction_timestamp
                })
        
        return pd.DataFrame(records)

    def save_to_gcs(self, df, output_dir="output"):
        if df.empty:
            self.logger.warning("No data to save.")
            return []

        os.makedirs(output_dir, exist_ok=True)

        run_ts_str = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        file_name = f"weather_batch_{run_ts_str}.csv"
        file_path = os.path.join(output_dir, file_name)

        try:
            df.to_csv(file_path, index=False)
            self.logger.info(f"Saved {len(df)} rows to {file_name}")

            blob_path = f"daily/{file_name}"
            blob = self.gcs_client.bucket(self.bucket_name).blob(blob_path)
            blob.upload_from_filename(file_path)
            self.logger.info(f"Uploaded to gs://{self.bucket_name}/{blob_path}")

            file_record = [{
                "run_ts": run_ts_str,
                "file_name": file_name,
                "rows": len(df),
                "status": "uploaded"
            }]
            
            log_file = save_run_log(file_record)
            try:
                upload_log_to_gcs(log_file, self.bucket_name)
                self.logger.info(
                    f"Uploaded run log to gs://{self.bucket_name}/logs/{os.path.basename(log_file)}"
                )
            except Exception as log_error:
                self.logger.error(f"CSV uploaded, but failed to upload run log: {log_error}")
                file_record[0]["log_upload_status"] = "failed"
                file_record[0]["log_upload_error"] = str(log_error)

            cleanup_local_folder(output_dir)
            return file_record

        except Exception as e:
            self.logger.error(f"Failed to process batch: {e}")
            raise

if __name__ == "__main__":
    args = parse_args()
    extractor = WeatherExtractor()
    
    results = extractor.extract(
        start_date=args.start_date, 
        end_date=args.end_date,
        load_historic=args.load_historic
    )
    
    df = extractor.parse_to_dataframe(results)
    extractor.save_to_gcs(df)
