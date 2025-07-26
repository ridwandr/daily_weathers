# etl_main.py

import os
import logging
from dotenv import load_dotenv
from prefect import flow
from etl.extract import fetch_weather_all_cities
from etl.transform import clean_weather_data, enrich_weather_data
from etl.load import upload_to_bigquery

# Load environment variables
load_dotenv(dotenv_path="config/.env")

# Logging setup
logging.basicConfig(level=logging.INFO)

@flow(name="OWM ETL Pipeline", log_prints=True)
def run_pipeline(mode: str = "append") -> None:
    """
    Prefect-deployable ETL flow for OpenWeatherMap data.
    """
    logging.info("Starting Prefect ETL Pipeline...")

    raw = fetch_weather_all_cities()
    if raw.empty:
        logging.warning("No data fetched. Exiting.")
        return

    cleaned = clean_weather_data(raw)
    enriched = enrich_weather_data(cleaned)

    if enriched.empty:
        logging.warning("No data after transform. Exiting.")
        return

    upload_to_bigquery(enriched, if_exists=mode)

    logging.info("✅ ETL Pipeline finished.")

# Optional CLI run
if __name__ == "__main__":
    run_pipeline()
