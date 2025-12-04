#!/bin/bash
# Setup script for Stock Market Data Pipeline
# This script helps with initial setup and troubleshooting

set -e

echo "🚀 Stock Market Data Pipeline Setup"
echo "===================================="
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker Desktop and try again."
    exit 1
fi

echo "✅ Docker is running"
echo ""

# Check if containers are already running
if docker ps | grep -q "airflow_stocks"; then
    echo "⚠️  Airflow container is already running"
    read -p "Do you want to stop and restart? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "Stopping containers..."
        docker compose down
    else
        echo "Skipping restart..."
        exit 0
    fi
fi

echo "📦 Building and starting containers..."
docker compose up --build -d

echo ""
echo "⏳ Waiting for Airflow to initialize (this may take 1-2 minutes)..."
sleep 30

# Check if Airflow user exists
echo ""
echo "🔍 Checking Airflow user..."
if docker exec airflow_stocks airflow users list 2>/dev/null | grep -q "airflow"; then
    echo "✅ Airflow user already exists"
else
    echo "👤 Creating Airflow admin user..."
    docker exec airflow_stocks airflow users create \
        --role Admin \
        --username airflow \
        --password airflow \
        --firstname Admin \
        --lastname User \
        --email admin@example.com || echo "⚠️  User creation failed (may already exist)"
fi

# Enable the DAG
echo ""
echo "📋 Enabling DAG..."
docker exec airflow_stocks airflow dags unpause yahoo_stock_pipeline || echo "⚠️  DAG may already be enabled"

echo ""
echo "✅ Setup complete!"
echo ""
echo "🌐 Access Airflow UI at: http://localhost:8080"
echo "   Username: airflow"
echo "   Password: airflow"
echo ""
echo "📊 To view database data:"
echo "   docker exec -it stocks_postgres psql -U airflow -d airflow -c \"SELECT * FROM market_data ORDER BY created_at DESC LIMIT 10;\""
echo ""

