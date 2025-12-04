import json
import os
from datetime import datetime, timedelta

import requests
import psycopg2
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook


DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}


def get_symbols() -> str:
    """Get comma-separated symbols from env, fallback to default."""
    env_symbols = os.environ.get("STOCK_SYMBOLS")
    if env_symbols:
        return env_symbols
    return "AAPL,MSFT,GOOGL"


with DAG(
    dag_id="yahoo_stock_pipeline",
    default_args=DEFAULT_ARGS,
    description="Fetch stock data from Yahoo Finance and store in PostgreSQL",
    schedule_interval="0 * * * *",  # hourly; change to "0 0 * * *" for daily
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["stocks", "yahoo", "example"],
) as dag:

    @task
    def fetch_stock_data() -> dict:
        """
        Fetch JSON data from Yahoo Finance quote API.
        Robustness:
        - Raises on non-200 HTTP.
        - Handles empty/invalid response.
        """
        symbols = get_symbols()
        url = "https://query1.finance.yahoo.com/v7/finance/quote"
        params = {"symbols": symbols}

        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
        except requests.RequestException as exc:
            # This will trigger Airflow retry due to raised exception
            raise RuntimeError(f"Error fetching Yahoo Finance data: {exc}") from exc

        try:
            data = response.json()
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Invalid JSON from Yahoo Finance: {exc}") from exc

        if "quoteResponse" not in data or "result" not in data["quoteResponse"]:
            raise RuntimeError(f"Unexpected Yahoo Finance structure: {data}")

        return data

    @task
    def transform_stock_data(raw_data: dict) -> list:
        """
        Extract relevant fields safely.
        Returns list of normalized records ready for DB load.
        Handles missing fields gracefully by setting them to None.
        """
        results = raw_data.get("quoteResponse", {}).get("result", [])
        if not isinstance(results, list):
            raise RuntimeError(f"Unexpected 'result' type: {type(results)}")

        transformed = []
        for item in results:
            try:
                symbol = item.get("symbol")
                price = item.get("regularMarketPrice")
                currency = item.get("currency")
                volume = item.get("regularMarketVolume")

                # market time in seconds since epoch (if present)
                market_time_raw = item.get("regularMarketTime")
                if market_time_raw:
                    market_time = datetime.utcfromtimestamp(int(market_time_raw))
                else:
                    # Fall back to now; keep pipeline running even if missing
                    market_time = datetime.utcnow()

                if not symbol:
                    # Skip records without symbol, but don't fail entire DAG
                    continue

                transformed.append(
                    {
                        "symbol": symbol,
                        "price": price,
                        "currency": currency,
                        "volume": volume,
                        "market_time": market_time.isoformat(),
                        "raw_json": item,
                    }
                )
            except Exception as exc:
                # Defensive: log and skip bad records instead of failing all
                # Airflow will still show this in task logs.
                print(f"Skipping malformed record {item}: {exc}")

        if not transformed:
            raise RuntimeError("No valid records extracted from Yahoo Finance data")

        return transformed

    @task
    def load_stock_data(records: list) -> None:
        """
        Upsert records into PostgreSQL (market_data table).
        Uses Airflow connection 'stocks_db' defined via AIRFLOW_CONN_STOCKS_DB env var.
        """
        if not records:
            # Nothing to load; do not fail
            print("No records to load.")
            return

        # Get connection from Airflow
        conn = BaseHook.get_connection("stocks_db")
        dsn = conn.get_uri().replace("postgresql+psycopg2://", "postgresql://")

        try:
            with psycopg2.connect(dsn) as pg_conn:
                with pg_conn.cursor() as cur:
                    upsert_sql = """
                        INSERT INTO market_data (
                            symbol, price, currency, volume, market_time, raw_json, updated_at
                        )
                        VALUES (%(symbol)s, %(price)s, %(currency)s, %(volume)s,
                                %(market_time)s, %(raw_json)s, NOW())
                        ON CONFLICT (symbol, market_time)
                        DO UPDATE SET
                            price      = EXCLUDED.price,
                            currency   = EXCLUDED.currency,
                            volume     = EXCLUDED.volume,
                            raw_json   = EXCLUDED.raw_json,
                            updated_at = NOW();
                    """

                    for r in records:
                        # Convert ISO string back to datetime in DB layer
                        r_local = dict(r)
                        r_local["market_time"] = datetime.fromisoformat(r["market_time"])
                        r_local["raw_json"] = json.dumps(r["raw_json"])
                        cur.execute(upsert_sql, r_local)
        except psycopg2.Error as exc:
            # Bubble up so Airflow can retry
            raise RuntimeError(f"Error inserting into PostgreSQL: {exc}") from exc

    # Define task dependencies
    raw = fetch_stock_data()
    cleaned = transform_stock_data(raw)
    load_stock_data(cleaned)