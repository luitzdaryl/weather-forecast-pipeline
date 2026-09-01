import re
import requests
import pandas as pd

OLLAMA_BASE_URL = "http://localhost:11434"

SCHEMA_CONTEXT = """
Table: weather_db.cleaned.weather_observations_cleaned

Columns:
- CITY (string) — name of the city
- OBSERVED_AT (timestamp) — when the reading was taken
- TEMPERATURE_C (float) — temperature in Celsius
- HUMIDITY_PCT (float) — relative humidity percentage
- WIND_SPEED_KMH (float) — wind speed in km/h
- WEATHER_CODE (int) — raw WMO weather code
- WEATHER_DESCRIPTION (string) — human-readable weather description, e.g. "Clear sky", "Light drizzle"
- TEMP_ROLLING_AVG_C (float) — 3-reading rolling average temperature
- INGESTED_AT (timestamp) — when the row was loaded into Snowflake

Note: OBSERVED_AT is stored in UTC. When filtering by "today", "this week", etc.,
compare against CURRENT_TIMESTAMP() converted to UTC, not CURRENT_DATE() directly.
"""

FORBIDDEN_KEYWORDS = [
    "DROP", "DELETE", "UPDATE", "INSERT", "ALTER",
    "TRUNCATE", "MERGE", "CREATE", "GRANT", "REVOKE", "EXEC", "CALL",
]

def get_ollama_models():
    """Same pattern as the original chat app: ask Ollama what's installed."""
    response = requests.get(f"{OLLAMA_BASE_URL}/api/tags", timeout=5)
    response.raise_for_status()
    data = response.json()
    return [m["name"] for m in data.get("models", [])]


def _ollama_generate(model, prompt, system=None):
    """One non-streaming call — we need the FULL response before we can
    validate/execute SQL, so streaming doesn't make sense for this step."""
    payload = {"model": model, "prompt": prompt, "stream": False}
    if system:
        payload["system"] = system
    response = requests.post(f"{OLLAMA_BASE_URL}/api/generate", json=payload, timeout=60)
    response.raise_for_status()
    return response.json()["response"].strip()


def _extract_sql(raw_text):
    """Models often wrap SQL in ```sql fences even when told not to — strip
    those if present, rather than assuming the raw output is clean."""
    match = re.search(r"```(?:sql)?\s*(.*?)```", raw_text, re.DOTALL)
    sql = match.group(1) if match else raw_text
    return sql.strip().rstrip(";")


def generate_sql(model, question):
    system = (
        "You are a SQL assistant for a Snowflake weather database. "
        "Given a question, output ONLY a single SQL SELECT query that answers it. "
        "No explanation, no markdown, just the raw SQL.\n\n"
        f"{SCHEMA_CONTEXT}"
    )
    raw = _ollama_generate(model, question, system=system)
    return _extract_sql(raw)


def is_safe_select(sql):
    """Read-only guardrail: reject anything that isn't a plain SELECT."""
    normalized = sql.strip().upper()
    if not normalized.startswith("SELECT"):
        return False, "Query does not start with SELECT."
    for keyword in FORBIDDEN_KEYWORDS:
        if re.search(rf"\b{keyword}\b", normalized):
            return False, f"Query contains forbidden keyword: {keyword}"
    return True, ""


def generate_answer(model, question, sql, result_df):
    preview = result_df.head(20).to_csv(index=False)
    prompt = (
        f"Question: {question}\n\n"
        f"SQL used: {sql}\n\n"
        f"Query results (CSV, up to 20 rows):\n{preview}\n\n"
        "Answer the question in plain, friendly language based on this data. "
        "Be concise and specific with numbers."
    )
    return _ollama_generate(model, prompt)