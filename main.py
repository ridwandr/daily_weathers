# main.py

import os
import logging
from dotenv import load_dotenv
from etl.extract import fetch_weather_all_cities
from etl.transform import clean_weather_data, enrich_weather_data
from etl.load import upload_to_bigquery

# === Load konfigurasi ===
load_dotenv(dotenv_path="config/.env")

# === Logging sederhana ===
logging.basicConfig(level=logging.INFO)

def run_pipeline(mode: str = "append"):
    """
    Jalankan pipeline ETL lokal:
    1. Extract → ambil data cuaca dari OpenWeatherMap
    2. Transform → bersihkan & perkaya data
    3. Load → unggah ke BigQuery
    """
    logging.info("🚀 Starting ETL pipeline...")

    # Extract
    raw_data = fetch_weather_all_cities()
    if raw_data.empty:
        logging.error("No data fetched from API. Pipeline stopped.")
        return

    # Transform
    cleaned = clean_weather_data(raw_data)
    enriched = enrich_weather_data(cleaned)

    if enriched.empty:
        logging.error("Transformed data is empty. Pipeline stopped.")
        return

    # Load
    upload_to_bigquery(enriched, if_exists=mode)

    logging.info("✅ ETL pipeline completed.")

if __name__ == "__main__":
    run_pipeline()
