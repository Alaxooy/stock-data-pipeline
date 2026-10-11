# Stock Market ETL Pipeline

A small ETL project that pulls stock market data into PostgreSQL using Apache Airflow. The pipeline fetches data from a public market API, cleans it, and stores it in a database that can later be connected to tools like Tableau or Power BI.

## Overview

This project demonstrates a simple data pipeline flow:

- API source fetches stock market data
- Airflow orchestrates the tasks
- Python transforms the raw response
- PostgreSQL stores the final records
- Tableau or BI tools can connect to the database for dashboards

## What this project does

The DAG named `yahoo_stock_pipeline` performs three steps:

1. `fetch_market_data`
   - tries the Yahoo Finance API first
   - falls back to Twelve Data if Yahoo rate-limits the request
   - keeps the request lightweight and stable for local testing

2. `clean_market_data`
   - extracts the important fields
   - normalizes prices and timestamps
   - drops malformed rows safely

3. `save_market_data`
   - uses PostgreSQL `INSERT ... ON CONFLICT` to upsert records
   - stores raw JSON and normalized data in `market_data`

## Project structure

```text
stock-data-pipeline/
├── dags/
│   └── yahoo_stock_pipeline.py
├── sql/
│   └── init_market_db.sql
├── docker-compose.yml
├── Dockerfile.airflow
├── requirements.txt
├── README.md
├── setup.sh
├── .env.example
├── .gitignore
└── logs/
```

## Prerequisites

- Docker Desktop
- Git
- A browser for the Airflow UI

## Quick start

### 1. Clone the repository

```bash
git clone <your-repo-url>
cd stock-data-pipeline
```

### 2. Start Docker

Make sure Docker Desktop is running on your machine.

### 3. Launch the pipeline

```bash
docker compose up --build -d
```

This will start:

- PostgreSQL on port `5432`
- Airflow on port `8080`

### 4. Open Airflow UI

Open:

```text
http://localhost:8080
```

Login with:

- Username: `airflow`
- Password: `airflow`

### 5. Trigger the DAG

In the Airflow UI:

1. Find `yahoo_stock_pipeline`
2. Click the play button to run it manually
3. Watch the tasks complete

## Default credentials

```text
Username: airflow
Password: airflow
```

## Database schema

The database table is created by `sql/init_market_db.sql`:

```sql
CREATE TABLE IF NOT EXISTS market_data (
    symbol        TEXT        NOT NULL,
    price         NUMERIC     NULL,
    currency      TEXT        NULL,
    volume        BIGINT      NULL,
    market_time   TIMESTAMPTZ NOT NULL,
    raw_json      JSONB       NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT market_data_pk PRIMARY KEY (symbol, market_time)
);

CREATE INDEX IF NOT EXISTS idx_market_data_time
    ON market_data (market_time DESC);
```

## Querying data

### Check rows from PostgreSQL

```bash
docker exec -it stocks_postgres psql -U airflow -d airflow -c "SELECT * FROM market_data ORDER BY created_at DESC LIMIT 10;"
```

### Example query

```sql
SELECT symbol, price, currency, volume, market_time
FROM market_data
WHERE symbol = 'AAPL'
ORDER BY market_time DESC
LIMIT 10;
```

## Export data to CSV

```bash
docker exec -it stocks_postgres psql -U airflow -d airflow -c "\COPY (SELECT * FROM market_data ORDER BY created_at DESC) TO '/tmp/market_data.csv' WITH CSV HEADER"
docker cp stocks_postgres:/tmp/market_data.csv .
```

This creates a file called `market_data.csv` in your project folder.

## Tableau / BI use

This project is designed to feed a dashboard tool. The `market_data` table is ready to use as a data source in Tableau, Power BI, or Python analysis notebooks.

Typical Tableau connection settings:

- Host: `localhost`
- Port: `5432`
- Database: `airflow`
- Username: `airflow`
- Password: `airflow`

## Configuration

### Environment variables

You can set these in a `.env` file if needed:

```env
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

AIRFLOW_USERNAME=airflow
AIRFLOW_PASSWORD=airflow
STOCK_SYMBOLS=AAPL
```

### Schedule

The DAG is currently manual-only for stability during local testing:

```python
schedule_interval=None
```

If you want hourly runs later, change it to:

```python
schedule_interval="0 * * * *"
```

## Troubleshooting

### Invalid login

Use:

- Username: `airflow`
- Password: `airflow`

If login still fails, recreate the container volume:

```bash
docker compose down -v
docker compose up --build -d
```

### Yahoo rate limiting

This project tries Yahoo first, then falls back to Twelve Data. If you want a more stable production setup, use a paid API key or switch to a different provider.

### DAG fails

Check the task logs in the Airflow UI for the exact error and verify that Docker containers are still running.

## Notes

- This is an ETL starter project, not a full trading platform.
- It is built to capture market data into PostgreSQL for later analytics.
- The database is the main output of the pipeline.

## License

This project is for educational and personal use.

