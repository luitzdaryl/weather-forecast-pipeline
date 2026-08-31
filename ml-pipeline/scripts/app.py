import os
import sys
import joblib
import pandas as pd
import snowflake.connector
import streamlit as st
from dotenv import load_dotenv
from chatbot import get_ollama_models, generate_sql, is_safe_select, generate_answer

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from train_model import add_hour_features  # reuse the EXACT training logic

load_dotenv(os.path.join(SCRIPT_DIR, "..", "..", "data-pipeline", ".env"))
MODEL_PATH = os.path.join(SCRIPT_DIR, "..", "models", "temp_forecast_model.pkl")

st.set_page_config(page_title="Weather Forecast Dashboard", page_icon="🌤️")


@st.cache_resource
def load_model():
    bundle = joblib.load(MODEL_PATH)
    return bundle["model"], bundle["feature_cols"]


@st.cache_data(ttl=300)
def load_recent_data(city="Taipei", limit=100):
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema="CLEANED",
    )
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT CITY, OBSERVED_AT, TEMPERATURE_C, HUMIDITY_PCT, "
            "WIND_SPEED_KMH, WEATHER_DESCRIPTION, TEMP_ROLLING_AVG_C "
            "FROM weather_observations_cleaned "
            "WHERE CITY = %s ORDER BY OBSERVED_AT DESC LIMIT %s",
            (city, limit),
        )
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=columns)
        return df.sort_values("OBSERVED_AT").reset_index(drop=True)
    finally:
        conn.close()


st.title("🌤️ Weather Forecast Dashboard")

city = st.selectbox("City", ["Taipei"])
df = load_recent_data(city)

if df.empty:
    st.warning("No data yet — check back after the pipeline runs.")
    st.stop()

latest = df.iloc[-1]

col1, col2, col3 = st.columns(3)
col1.metric("Latest Temperature", f"{latest['TEMPERATURE_C']:.1f} °C")
col2.metric("Humidity", f"{latest['HUMIDITY_PCT']:.0f}%")
col3.metric("Conditions", latest["WEATHER_DESCRIPTION"])
st.caption(f"Last observed: {latest['OBSERVED_AT']}")

st.subheader("Temperature history")
st.line_chart(df.set_index("OBSERVED_AT")["TEMPERATURE_C"])

st.subheader("Next-hour forecast")

model, feature_cols = load_model()

latest_df = add_hour_features(pd.DataFrame([latest]))
X_live = latest_df[feature_cols]
prediction = model.predict(X_live)[0]
delta = prediction - latest["TEMPERATURE_C"]

st.metric(
    "Predicted temperature (next hour)",
    f"{prediction:.1f} °C",
    delta=f"{delta:+.1f} °C vs now",
)

st.caption(
    "Random Forest model, trained on Snowflake's cleaned weather data. "
    "Evaluated against a naive persistence baseline during training — "
    "see ml-pipeline/scripts/train_model.py."
)