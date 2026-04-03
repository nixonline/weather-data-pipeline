import os
import sys
import json
import yaml
import logging
import pandas as pd
from datetime import datetime, UTC
from dotenv import load_dotenv
from google.cloud import storage
from google.oauth2 import service_account

# Resolve imports from the project root so shared modules are loaded consistently.
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SCRIPT_DIR)
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

PROJECT_ID = "weather-data-etl-491716"

from shared.gcp import download_weather_data, upsert_transformed_weather_to_bq
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
            self.columns_to_exclude = config.get("columns_to_exclude", [])
            self.logger.info(f"Loaded {len(self.weather_conditions)} weather conditions from config.")
            self.logger.info(f"Loaded schema with {len(self.schema)} columns.")
        except Exception as e:
            self.logger.error(f"Error parsing config.yaml: {e}")
            raise

    def transform(self, load_historic=False):
        """
        Transforms raw weather data by mapping condition codes to descriptions and adding rolling averages.
        
        Args:
            load_historic: If True, download all historical files. If False, download top 7 files.
        Returns:
            pd.DataFrame: Transformed weather data with weather_desc and rolling average columns.
        """
        local_dir = os.path.join(self.root_dir, "gcs_data")

        # Always load full history to ensure we have 7 days for rolling calculations
        download_weather_data(
            bucket_name=self.bucket_name, 
            blob_prefix="daily/", 
            local_dir=local_dir, 
            load_historic=load_historic
        )

        df_weather = load_files_and_append_to_df(local_dir)
        
        if df_weather.empty:
            self.logger.warning("No weather data found.")
            return df_weather

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
        if "weather_code" in df_weather.columns:
            df_weather["weather_desc"] = df_weather["weather_code"].map(self.weather_conditions)
        else:
            self.logger.warning("weather_code column not found in data.")

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

        # Remove columns not in schema
        schema_columns = list(self.schema.keys())
        schema_columns = [col for col in schema_columns if col not in self.columns_to_exclude]
        df_weather = df_weather[[col for col in schema_columns if col in df_weather.columns]]
        
        # Convert date column to date-only (remove time)
        if "date" in df_weather.columns:
            df_weather["date"] = pd.to_datetime(df_weather["date"]).dt.date
        
        # Round all float columns to 2 decimal places
        float_cols = df_weather.select_dtypes(include=['float64', 'float32']).columns
        for col in float_cols:
            df_weather[col] = df_weather[col].round(2)

        cleanup_local_folder(local_dir)

        return df_weather

if __name__ == "__main__":
    args = parse_args(description="Weather Data Transformer")

    transformer = WeatherTransformer()

    results = transformer.transform(
        load_historic=args.load_historic
    )

    if results.empty:
        transformer.logger.warning("No transformed data to upsert to BigQuery.")
    else:
        table_id = f"{PROJECT_ID}.weather_dataset.daily_weather"
        bq_result = upsert_transformed_weather_to_bq(results, table_id)
        transformer.logger.info(f"\nBigQuery Upsert Result: {bq_result}")
