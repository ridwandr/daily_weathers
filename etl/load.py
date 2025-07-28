# load.py

import pandas as pd
import os
import logging
from pandas_gbq import to_gbq
from google.oauth2 import service_account
from dotenv import load_dotenv

load_dotenv(dotenv_path="config/.env")
logging.basicConfig(level=logging.INFO)

PROJECT_ID = os.getenv("PROJECT_ID")
TABLE_ID = os.getenv("TABLE_ID")
CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

def upload_to_bigquery(
    df: pd.DataFrame,
    project_id: str = PROJECT_ID,
    table_id: str = TABLE_ID,
    credentials_path: str = CREDENTIALS_PATH,
    if_exists: str = "append"
):
    """
    Unggah DataFrame ke Google BigQuery.

    Parameters:
        df (pd.DataFrame): Data yang akan diunggah
        project_id (str): ID project GCP
        table_id (str): Format dataset.table
        credentials_path (str): Path ke JSON file service account
        if_exists (str): Mode upload: 'replace', 'append', 'fail'
    """
    try:
        credentials = service_account.Credentials.from_service_account_file(credentials_path)
        logging.info(f"Uploading to BigQuery: {table_id} (mode: {if_exists})")
        to_gbq(
            dataframe=df,
            destination_table=table_id,
            project_id=project_id,
            credentials=credentials,
            if_exists=if_exists
        )
        logging.info("✅ Upload success.")
    except Exception as e:
        logging.error(f"❌ Upload failed: {e}")
