import json
import os
from datetime import datetime, timedelta

import psycopg2
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.utils.dates import days_ago


DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}


def get_symbols() -> str:
    """Return configured stock symbols, defaulting to one safe symbol."""
    symbols = os.environ.get("STOCK_SYMBOLS")
    return symbols if symbols else "AAPL"


def fetch_from_twelve_data(symbol: str) -> dict:
    """Fetch a stock quote from Twelve Data."""
    response = requests.get(
        "https://api.twelvedata.com/quote",
        params={"symbol": symbol.upper(), "apikey": "demo"},
        timeout=20,
        headers={"User-Agent": "Mozilla/5.0"},
    )
    response.raise_for_status()

    data = response.json()
    if "symbol" not in data:
        raise RuntimeError(f"Unexpected Twelve Data response for {symbol}: {data}")

    price = data.get("close") or data.get("last") or data.get("price")
    if price is None:
        raise RuntimeError(f"Missing price in Twelve Data response for {symbol}: {data}")

    timestamp = data.get("timestamp") or datetime.utcnow().timestamp()

    return {
        "quoteResponse": {
            "result": [
                {
                    "symbol": symbol.upper(),
                    "regularMarketPrice": float(price),
                    "currency": data.get("currency", "USD"),
                    "regularMarketVolume": int(data.get("volume") or 0),
                    "regularMarketTime": int(timestamp),
                }
            ]
        }
    }


with DAG(
    dag_id="stock_market_pipeline",
    default_args=DEFAULT_ARGS,
    description="Fetch stock prices and store them in PostgreSQL.",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["stocks", "etl"],
) as dag:

    @task
    def fetch_market_data() -> dict:
        """Fetch stock data from Twelve Data."""
        symbols = [symbol.strip() for symbol in get_symbols().split(",") if symbol.strip()]
        if not symbols:
            raise RuntimeError("No stock symbols configured.")

        symbol = symbols[0]
        try:
            return fetch_from_twelve_data(symbol)
        except Exception as exc:
            raise RuntimeError(f"Twelve Data fetch failed for {symbol}: {exc}") from exc

    @task
    def clean_market_data(raw_data: dict) -> list:
        """Normalize API output into rows ready for insertion."""
        results = raw_data.get("quoteResponse", {}).get("result", [])
        if not isinstance(results, list):
            raise RuntimeError(f"Unexpected result type: {type(results)}")

        cleaned = []
        for item in results:
            try:
                symbol = item.get("symbol")
                price = item.get("regularMarketPrice")
                currency = item.get("currency")
                volume = item.get("regularMarketVolume")
                market_time_raw = item.get("regularMarketTime")

                if not symbol:
                    continue

                market_time = (
                    datetime.utcfromtimestamp(int(market_time_raw))
                    if market_time_raw
                    else datetime.utcnow()
                )

                cleaned.append(
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
                print(f"Skipping malformed record {item}: {exc}")

        if not cleaned:
            raise RuntimeError("No valid records extracted from market data.")

        return cleaned

    @task
    def save_market_data(records: list) -> None:
        """Insert records into PostgreSQL with an upsert."""
        if not records:
            print("No records to save.")
            return

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

                    for row in records:
                        record = dict(row)
                        record["market_time"] = datetime.fromisoformat(row["market_time"])
                        record["raw_json"] = json.dumps(row["raw_json"])
                        cur.execute(upsert_sql, record)
        except psycopg2.Error as exc:
            raise RuntimeError(f"Error inserting into PostgreSQL: {exc}") from exc

    raw_data = fetch_market_data()
    cleaned_data = clean_market_data(raw_data)
    save_market_data(cleaned_data)
