# etl_main.py

import os
import logging
from dotenv import load_dotenv
from prefect import flow
from datetime import datetime
from etl.extract import fetch_weather_all_cities
from etl.transform import clean_weather_data, enrich_weather_data
from etl.load import upload_to_bigquery

# Load environment variables
load_dotenv()

# Logging Setup
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"etl_log_{datetime.now().strftime('%Y%m%d')}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

@flow(name="OWM Pipeline", log_prints=True)
def run_pipeline(mode: str = "append") -> None:
    """
    Prefect-deployable ETL flow for OpenWeatherMap data.
    """
    print("Running run_pipeline function...")
    
    logging.info(f"Starting ETL Pipeline - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ")
    raw = fetch_weather_all_cities()
    logging.info(f"Extracted data shape: {raw.shape}")

    if raw.empty:
        logging.warning("No data fetched. Exiting.")
        return
    
    logging.info(f"--Data Cleanup")
    cleaned = clean_weather_data(raw)
    logging.info(f"--Data Enrichment")
    enriched = enrich_weather_data(cleaned)

    if enriched.empty:
        logging.warning("No data after transform. Exiting.")
        return

    PROJECT_ID = os.getenv("PROJECT_ID")
    TABLE_ID = os.getenv("TABLE_ID")

    upload_to_bigquery(enriched, PROJECT_ID, TABLE_ID, if_exists=mode)

    logging.info("ETL Pipeline finished.")

# Optional CLI run
if __name__ == "__main__":
    run_pipeline()
