import os
import requests
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()  # reads .env and loads its keys into os.environ

CITY = "Taipei"
LATITUDE = 25.0330
LONGITUDE = 121.5654


def fetch_weather():
    """Call Open-Meteo and return one current weather reading."""
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,weather_code",
        "timezone": "UTC",
    }
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()  # throws an error if Open-Meteo returns a bad status
    current = response.json()["current"]

    return {
        "city": CITY,
        "observed_at": current["time"],
        "temperature_c": current["temperature_2m"],
        "humidity_pct": current["relative_humidity_2m"],
        "wind_speed_kmh": current["wind_speed_10m"],
        "weather_code": current["weather_code"],
    }


def insert_into_snowflake(reading):
    """Open a connection, insert one row, close the connection."""
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
    )
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO weather_observations
                (city, observed_at, temperature_c, humidity_pct, wind_speed_kmh, weather_code)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                reading["city"],
                reading["observed_at"],
                reading["temperature_c"],
                reading["humidity_pct"],
                reading["wind_speed_kmh"],
                reading["weather_code"],
            ),
        )
        conn.commit()
        print(f"Inserted reading for {reading['city']} at {reading['observed_at']}")
    finally:
        conn.close()


if __name__ == "__main__":
    reading = fetch_weather()
    print("Fetched:", reading)
    insert_into_snowflake(reading)