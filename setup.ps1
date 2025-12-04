# PowerShell Setup script for Stock Market Data Pipeline
# This script helps with initial setup and troubleshooting on Windows

Write-Host "🚀 Stock Market Data Pipeline Setup" -ForegroundColor Cyan
Write-Host "====================================" -ForegroundColor Cyan
Write-Host ""

# Check if Docker is running
try {
    docker info | Out-Null
    Write-Host "✅ Docker is running" -ForegroundColor Green
} catch {
    Write-Host "❌ Docker is not running. Please start Docker Desktop and try again." -ForegroundColor Red
    exit 1
}

Write-Host ""

# Check if containers are already running
$airflowRunning = docker ps | Select-String "airflow_stocks"
if ($airflowRunning) {
    Write-Host "⚠️  Airflow container is already running" -ForegroundColor Yellow
    $response = Read-Host "Do you want to stop and restart? (y/n)"
    if ($response -eq "y" -or $response -eq "Y") {
        Write-Host "Stopping containers..." -ForegroundColor Yellow
        docker compose down
    } else {
        Write-Host "Skipping restart..." -ForegroundColor Yellow
        exit 0
    }
}

Write-Host "📦 Building and starting containers..." -ForegroundColor Cyan
docker compose up --build -d

Write-Host ""
Write-Host "⏳ Waiting for Airflow to initialize (this may take 1-2 minutes)..." -ForegroundColor Yellow
Start-Sleep -Seconds 30

# Check if Airflow user exists
Write-Host ""
Write-Host "🔍 Checking Airflow user..." -ForegroundColor Cyan
$userCheck = docker exec airflow_stocks airflow users list 2>$null | Select-String "airflow"
if ($userCheck) {
    Write-Host "✅ Airflow user already exists" -ForegroundColor Green
} else {
    Write-Host "👤 Creating Airflow admin user..." -ForegroundColor Cyan
    docker exec airflow_stocks airflow users create `
        --role Admin `
        --username airflow `
        --password airflow `
        --firstname Admin `
        --lastname User `
        --email admin@example.com
    if ($LASTEXITCODE -ne 0) {
        Write-Host "⚠️  User creation failed (may already exist)" -ForegroundColor Yellow
    }
}

# Enable the DAG
Write-Host ""
Write-Host "📋 Enabling DAG..." -ForegroundColor Cyan
docker exec airflow_stocks airflow dags unpause yahoo_stock_pipeline 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "⚠️  DAG may already be enabled" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "✅ Setup complete!" -ForegroundColor Green
Write-Host ""
Write-Host "🌐 Access Airflow UI at: http://localhost:8080" -ForegroundColor Cyan
Write-Host "   Username: airflow" -ForegroundColor White
Write-Host "   Password: airflow" -ForegroundColor White
Write-Host ""
Write-Host "📊 To view database data:" -ForegroundColor Cyan
Write-Host "   docker exec -it stocks_postgres psql -U airflow -d airflow -c `"SELECT * FROM market_data ORDER BY created_at DESC LIMIT 10;`"" -ForegroundColor White
Write-Host ""

