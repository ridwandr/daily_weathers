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

# def fetch_weather_all_cities(city_file: str = CITY_LIST_PATH) -> pd.DataFrame:
#     import os
# import pandas as pd
# import requests
# import logging

def fetch_weather_all_cities():
    logging.info("🌐 Starting weather data fetch...")
    
    api_key = os.getenv("OWM_API_KEY")
    if not api_key:
        logging.warning("❌ OWM_API_KEY not found in environment!")
        return pd.DataFrame()

    try:
        cities = ["Jakarta", "Surabaya", "Bandung"]  # ⬅️ Sementara, ganti file CSV
        records = []

        for city in cities:
            logging.info(f"Fetching weather for: {city}")
            url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
            r = requests.get(url)
            if r.status_code != 200:
                logging.warning(f"Failed to fetch for {city}: {r.status_code}")
                continue

            data = r.json()
            record = {
                "city": city,
                "temperature": data["main"]["temp"],         # ✅ ganti dari temp → temperature
                "humidity": data["main"]["humidity"],
                "weather": data["weather"][0]["description"],
                "timestamp": pd.Timestamp.utcnow()           # ✅ ganti dari fetched_at → timestamp
            }
            records.append(record)

        df = pd.DataFrame(records)
        logging.info(f"✅ Fetched {len(df)} records.")
        return df

    except Exception as e:
        logging.error(f"❌ Exception during fetch: {e}")
        return pd.DataFrame()

    # logging.info("🌐 Fetch function called.")
    # API_KEY = os.getenv("OWM_API_KEY")
    # logging.info(f"API KEY from env: {API_KEY}")

    # path = "config/city_list.csv"
    # logging.info(f"Checking CSV path: {path}")
    # assert os.path.exists(path), f"{path} not found"

    # df = pd.read_csv(path)
    # logging.info(f"Loaded {len(df)} cities")

    # cities_df = pd.read_csv(city_file)
    # records = []
    # for _, row in cities_df.iterrows():
    #     result = fetch_weather_by_id(row["id"])
    #     if result:
    #         records.append(result)
    #     else:
    #         logging.warning(f"Skipping city ID {row['id']} ({row['name']})")
    # return pd.DataFrame(records)


if __name__ == "__main__":
    df = fetch_weather_all_cities()
    print(df)
