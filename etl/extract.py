# etl/extract.py

import requests
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv
import logging

load_dotenv(dotenv_path="config/.env")

logging.basicConfig(level=logging.INFO)

API_KEY = os.getenv("OWM_API_KEY")
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"
CITY_LIST_PATH = "config/city_list.csv"

def fetch_weather_by_id(city_id: int) -> dict:
    """Fetch weather data by OpenWeatherMap city ID"""
    params = {
        "id": city_id,
        "appid": API_KEY,
        "units": "metric"
    }
    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()
        return {
            "city_id": data["id"],
            "city": data["name"],
            "country": data["sys"]["country"],
            "lat": data["coord"]["lat"],
            "lon": data["coord"]["lon"],
            "temperature": data["main"]["temp"],
            "humidity": data["main"]["humidity"],
            "weather": data["weather"][0]["main"],
            "description": data["weather"][0]["description"],
            "wind_speed": data["wind"]["speed"],
            "timestamp": datetime.utcfromtimestamp(data["dt"]),
            "fetched_at": datetime.utcnow()
        }
    except Exception as e:
        logging.error(f"Error fetching city ID {city_id}: {e}")
        return None

def fetch_weather_all_cities(city_file: str = CITY_LIST_PATH) -> pd.DataFrame:
    cities_df = pd.read_csv(city_file)
    records = []
    for _, row in cities_df.iterrows():
        result = fetch_weather_by_id(row["id"])
        if result:
            records.append(result)
        else:
            logging.warning(f"Skipping city ID {row['id']} ({row['name']})")
    return pd.DataFrame(records)


if __name__ == "__main__":
    df = fetch_weather_all_cities()
    print(df)
