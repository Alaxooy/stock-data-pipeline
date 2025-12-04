# Stock Market Data Pipeline

A Dockerized data pipeline using Apache Airflow to automatically fetch, parse, and store stock market data from Yahoo Finance into PostgreSQL.

<img width="1905" height="928" alt="stock airflow ui" src="https://github.com/user-attachments/assets/facc9ac0-fa46-465d-9fa6-5302ddd212f0" />


## 🎯 Features

- **Automated Data Fetching**: Retrieves stock market data from Yahoo Finance API on a scheduled basis (hourly by default)
- **Data Processing**: Parses JSON responses and extracts relevant stock information
- **Database Storage**: Stores data in PostgreSQL with automatic table creation
- **Error Handling**: Comprehensive error handling and retry logic for robustness
- **Dockerized**: Complete setup using Docker Compose for easy deployment
- **Environment Variables**: Secure management of credentials and configuration

## 📋 Prerequisites

- **Docker Desktop** (with WSL2 backend enabled) - [Download here](https://www.docker.com/products/docker-desktop)
- **Git** (optional, for cloning the repository)

## 🚀 Quick Start

### 1. Clone or Download the Repository

```bash
git clone <your-repo-url>
cd stock-pipeline
```

### 2. Configure Environment Variables (Optional)

Edit the `.env` file to customize settings:

```env
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

AIRFLOW_USERNAME=airflow
AIRFLOW_PASSWORD=airflow

# Comma-separated stock symbols to track
STOCK_SYMBOLS=AAPL,MSFT,GOOGL,AMZN

AIRFLOW_CONN_STOCKS_DB=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
```

### 3. Start the Pipeline

```bash
docker compose up --build
```

This command will:
- Build the Airflow Docker image with required dependencies
- Start PostgreSQL database container
- Initialize the database and create the `market_data` table
- Start Airflow webserver and scheduler
- Create Airflow admin user (if not exists)

**Note**: The first startup may take 2-3 minutes to download images and initialize.

### 4. Access Airflow Web UI

1. Open your browser and navigate to: `http://localhost:8080`
2. Login with credentials:
   - **Username**: `airflow`
   - **Password**: `airflow`

### 5. Enable and Run the DAG

1. In the Airflow UI, find the `yahoo_stock_pipeline` DAG
2. Toggle the switch on the left to **enable** the DAG (it should turn blue/green)
3. Click the **play button (▶)** to trigger a manual run
4. Click on the DAG name to view the Graph View and monitor task execution

## 🔧 Manual Setup (If Needed)

If the Airflow user wasn't created automatically during startup, you can create it manually:

```bash
# Create Airflow admin user
docker exec -it airflow_stocks airflow users create \
  --role Admin \
  --username airflow \
  --password airflow \
  --firstname Admin \
  --lastname User \
  --email admin@example.com

# Enable the DAG (if needed)
docker exec -it airflow_stocks airflow dags unpause yahoo_stock_pipeline
```

## 📊 Pipeline Architecture

The pipeline consists of three main tasks:

1. **`fetch_stock_data`**: Fetches JSON data from Yahoo Finance API
   - Uses `requests` library with timeout handling
   - Validates HTTP response status
   - Handles network errors with retries

2. **`transform_stock_data`**: Parses and extracts relevant fields
   - Extracts: symbol, price, currency, volume, market_time
   - Handles missing fields gracefully (sets to None)
   - Skips malformed records without failing entire pipeline

3. **`load_stock_data`**: Stores data in PostgreSQL
   - Uses upsert logic (INSERT ... ON CONFLICT UPDATE)
   - Prevents duplicate entries
   - Stores both normalized data and raw JSON

## 🗄️ Database Schema

The `market_data` table structure:

```sql
CREATE TABLE market_data (
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
```

## 🔍 Querying the Data

### Using Docker

```bash
# Connect to PostgreSQL and query data
docker exec -it stocks_postgres psql -U airflow -d airflow -c \
  "SELECT * FROM market_data ORDER BY created_at DESC LIMIT 10;"
```

### Using PostgreSQL Client

Connect with:
- **Host**: `localhost`
- **Port**: `5432`
- **Database**: `airflow`
- **Username**: `airflow`
- **Password**: `airflow`

Example query:
```sql
SELECT symbol, price, currency, volume, market_time 
FROM market_data 
WHERE symbol = 'AAPL' 
ORDER BY market_time DESC 
LIMIT 10;
```

## ⚙️ Configuration

### Schedule

The DAG runs **hourly** by default. To change the schedule, edit `dags/yahoo_stock_pipeline.py`:

```python
schedule_interval="0 * * * *",  # Hourly
# schedule_interval="0 0 * * *",  # Daily
# schedule_interval=None,  # Manual trigger only
```

### Stock Symbols

Modify the `STOCK_SYMBOLS` environment variable in `.env`:

```env
STOCK_SYMBOLS=AAPL,MSFT,GOOGL,AMZN,TSLA,NVDA
```

### Retry Configuration

The pipeline includes automatic retries:
- **Retries**: 3 attempts
- **Retry Delay**: 5 minutes between retries

Configure in `dags/yahoo_stock_pipeline.py`:

```python
DEFAULT_ARGS = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}
```

## 🛠️ Project Structure

```
stock-pipeline/
├── docker-compose.yml          # Docker Compose configuration
├── Dockerfile.airflow          # Airflow Docker image definition
├── requirements.txt            # Python dependencies
├── .env                        # Environment variables (not in git)
├── README.md                   # This file
├── dags/
│   └── yahoo_stock_pipeline.py # Main Airflow DAG
├── sql/
│   └── init_market_db.sql      # Database initialization script
├── logs/                       # Airflow logs (created automatically)
└── plugins/                    # Airflow plugins (empty by default)
```

## 🐛 Troubleshooting

### Airflow UI not accessible

- Ensure Docker containers are running: `docker ps`
- Check if port 8080 is already in use
- View logs: `docker logs airflow_stocks`

### DAG not appearing

- Check for import errors: `docker exec -it airflow_stocks airflow dags list-import-errors`
- Verify DAG file exists: `docker exec -it airflow_stocks ls -la /opt/airflow/dags/`
- Check Airflow logs for errors

### Database connection errors

- Ensure PostgreSQL container is running: `docker ps | grep postgres`
- Verify connection string in `.env` file
- Check PostgreSQL logs: `docker logs stocks_postgres`

### Task failures

- View task logs in Airflow UI (click on failed task → Log)
- Check for API rate limiting (Yahoo Finance may throttle requests)
- Verify network connectivity from container

## 🔒 Security Notes

- **Never commit** `.env` file to version control
- Change default passwords in production
- Use secrets management (e.g., Docker Secrets, AWS Secrets Manager) for production
- Restrict database access in production environments

## 📝 License

This project is provided as-is for educational and demonstration purposes.

## 🤝 Contributing

Feel free to submit issues or pull requests for improvements!

## 📧 Support

For issues or questions, please open an issue in the repository.

