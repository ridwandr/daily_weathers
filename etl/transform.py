# transform.py

import pandas as pd
from pytz import timezone
import numpy as np

# Default: Waktu Jakarta (bisa disesuaikan)
WIB = timezone("Asia/Jakarta")

def clean_weather_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Bersihkan dan transformasi data cuaca:
    - Pastikan tipe data konsisten
    - Drop null penting
    - Konversi timestamp ke timezone lokal
    """
    if df.empty:
        return df

    df = df.dropna(subset=["temperature", "humidity", "weather", "timestamp"])

    df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_localize("UTC").dt.tz_convert(WIB)
    df["fetched_at"] = pd.to_datetime(df["fetched_at"]).dt.tz_localize("UTC").dt.tz_convert(WIB)

    return df


def enrich_weather_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Tambahkan kolom turunan:
    - temp_category (dingin, hangat, panas)
    - local_time, day_of_week, hour_of_day
    """
    if df.empty:
        return df

    def classify_temp(temp):
        if temp < 20:
            return "Cold"
        elif temp < 30:
            return "Warm"
        else:
            return "Hot"

    df["temp_category"] = df["temperature"].apply(classify_temp)
    df["day_of_week"] = df["timestamp"].dt.day_name()
    df["hour_of_day"] = df["timestamp"].dt.hour

    return df


if __name__ == "__main__":
    from extract import fetch_weather_all_cities

    raw = fetch_weather_all_cities()
    clean = clean_weather_data(raw)
    enriched = enrich_weather_data(clean)

    print(enriched.head())
