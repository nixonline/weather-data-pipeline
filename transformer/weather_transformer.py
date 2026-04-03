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

from shared.gcp import upload_log_to_gcs, download_weather_data
from shared.utils import load_files_and_append_to_df, parse_args, resolve_dates, save_run_log, cleanup_local_folder, apply_schema_dtypes


class WeatherTransformer:
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
            self.logger.error(f"config.yaml missing in transformer folder: {config_path}")
            raise FileNotFoundError(f"Ensure config.yaml is moved to {self.script_dir}")

        try:
            with open(config_path, "r") as f:
                config = yaml.safe_load(f)
            config = config or {}
            self.weather_conditions = config.get("weather_conditions", {})
            self.schema = config.get("schema", {})
            self.logger.info(f"Loaded {len(self.weather_conditions)} weather conditions from config.")
            self.logger.info(f"Loaded schema with {len(self.schema)} columns.")
        except Exception as e:
            self.logger.error(f"Error parsing config.yaml: {e}")
            raise

    def transform(self, load_historic=False):
        """
        Transforms raw weather data by mapping condition codes to descriptions and adding rolling averages.
        
        Args:
            load_historic: If True, loads all historical data. If False, loads last 7 days for rolling averages.
        Returns:
            pd.DataFrame: Transformed weather data with weather_desc and rolling average columns.
        """
        local_dir = f"{self.root_dir}\gcs_data"

        # Always load full history to ensure we have 7 days for rolling calculations
        download_weather_data(
            bucket_name=self.bucket_name, 
            blob_prefix="daily/", 
            local_dir=local_dir, 
            load_historic=True
        )

        df_weather = load_files_and_append_to_df(local_dir)

        # Apply schema dtypes (handles all conversions including datetime)
        df_weather = apply_schema_dtypes(df_weather, self.schema)

        # Since forecasts are being included daily, 
        # we want to keep the most recent forecast for each city/date combination.
        key_cols = ["city", "country", "date"]
        df_weather = (
            df_weather
            .sort_values("extracted_at", ascending=False)
            .drop_duplicates(subset=key_cols, keep="first")
            .reset_index(drop=True)
        )

        # Adding weather description based on codes
        df_weather["weather_desc"] = df_weather["weather_code"].map(self.weather_conditions)

        # Calculate rolling averages for temperature by city after deduplication
        # Sort by city and date for proper rolling calculations
        df_weather = df_weather.sort_values(["city", "date"]).reset_index(drop=True)
        
        temp_cols = ["temp_max", "temp_min"]
        for col in temp_cols:
            if col in df_weather.columns:
                # 3-day and 7-day rolling averages by city
                df_weather[f"{col}_3day_avg"] = (
                    df_weather.groupby("city")[col].rolling(window=3, min_periods=1).mean().reset_index(0, drop=True)
                )
                df_weather[f"{col}_7day_avg"] = (
                    df_weather.groupby("city")[col].rolling(window=7, min_periods=1).mean().reset_index(0, drop=True)
                )

        return df_weather

if __name__ == "__main__":
    args = parse_args(description="Weather Data Transformer")
    extractor = WeatherTransformer()
    
    results = extractor.transform(
        load_historic=args.load_historic
    )
    
    print("cat")
