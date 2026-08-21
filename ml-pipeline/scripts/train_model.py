import os
import joblib
import numpy as np
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(SCRIPT_DIR, "..", "..", "data-pipeline", ".env"))
MODEL_PATH = os.path.join(SCRIPT_DIR, "..", "models", "temp_forecast_model.pkl")


def load_data():
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
            "WIND_SPEED_KMH, TEMP_ROLLING_AVG_C "
            "FROM weather_observations_cleaned ORDER BY city, observed_at"
        )
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=columns)
    finally:
        conn.close()


def engineer_features(df):
    df = df.sort_values(["CITY", "OBSERVED_AT"]).reset_index(drop=True)

    # Cyclical encoding of hour-of-day: raw hour (0-23) is misleading to a model,
    # since hour 23 and hour 0 are numerically far apart but temporally adjacent.
    # Representing hour as a point on a circle fixes this.
    hour = df["OBSERVED_AT"].dt.hour
    df["HOUR_SIN"] = np.sin(2 * np.pi * hour / 24)
    df["HOUR_COS"] = np.cos(2 * np.pi * hour / 24)

    # The target: next reading's temperature, per city (groupby prevents one
    # city's last row from leaking into another city's first row).
    df["TARGET_NEXT_TEMP"] = df.groupby("CITY")["TEMPERATURE_C"].shift(-1)

    # Drop rows with no target (each city's final row has no "next" reading yet)
    df = df.dropna(subset=["TARGET_NEXT_TEMP"])
    return df


def train_and_evaluate(df):
    feature_cols = ["TEMPERATURE_C", "HUMIDITY_PCT", "WIND_SPEED_KMH",
                     "TEMP_ROLLING_AVG_C", "HOUR_SIN", "HOUR_COS"]
    X = df[feature_cols]
    y = df["TARGET_NEXT_TEMP"]

    # Chronological split — NOT random. First 80% of rows (by time) train,
    # last 20% test. This mimics genuinely predicting the future, not
    # peeking at it.
    split_idx = int(len(df) * 0.8)
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

    # Baseline: "next hour's temp = current temp" — naive, but a real,
    # legitimate forecasting strategy. Any real model MUST beat this to
    # be worth using at all.
    baseline_pred = X_test["TEMPERATURE_C"]
    baseline_mae = mean_absolute_error(y_test, baseline_pred)

    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    model_pred = model.predict(X_test)
    model_mae = mean_absolute_error(y_test, model_pred)
    model_rmse = np.sqrt(mean_squared_error(y_test, model_pred))

    print(f"Train rows: {len(X_train)}, Test rows: {len(X_test)}")
    print(f"Baseline (persistence) MAE: {baseline_mae:.3f} °C")
    print(f"Model MAE:  {model_mae:.3f} °C")
    print(f"Model RMSE: {model_rmse:.3f} °C")

    if model_mae < baseline_mae:
        print("Model beats the naive baseline.")
    else:
        print("Model does NOT beat the naive baseline — needs more data or features.")

    return model, feature_cols


def main():
    df = load_data()
    print(f"Loaded {len(df)} rows")
    df = engineer_features(df)
    print(f"After feature engineering: {len(df)} rows")

    model, feature_cols = train_and_evaluate(df)

    joblib.dump({"model": model, "feature_cols": feature_cols}, MODEL_PATH)
    print(f"Model saved to {MODEL_PATH}")


if __name__ == "__main__":
    main()