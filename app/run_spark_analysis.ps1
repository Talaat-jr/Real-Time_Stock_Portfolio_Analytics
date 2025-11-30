# Run Spark Analysis Script (PowerShell)
# This script helps execute Spark analysis on the FULL_STOCKS.csv

Write-Host "==============================================" -ForegroundColor Cyan
Write-Host "Spark Stock Portfolio Analysis Runner" -ForegroundColor Cyan
Write-Host "==============================================" -ForegroundColor Cyan
Write-Host ""

# Check if FULL_STOCKS.csv exists
if (-Not (Test-Path "output\FULL_STOCKS.csv")) {
    Write-Host "❌ Error: output\FULL_STOCKS.csv not found" -ForegroundColor Red
    Write-Host "Please run the main pipeline first: docker-compose up app"
    exit 1
}

Write-Host "✅ Found FULL_STOCKS.csv" -ForegroundColor Green
Write-Host ""

# Create output directory for Spark results
New-Item -ItemType Directory -Force -Path "output\spark_results" | Out-Null
Write-Host "✅ Created output\spark_results directory" -ForegroundColor Green
Write-Host ""

# Check if Spark master is running
Write-Host "🔍 Checking if Spark master is running..." -ForegroundColor Yellow
$sparkMaster = docker ps --filter "name=spark-master" --format "{{.Names}}"
if (-Not $sparkMaster) {
    Write-Host "❌ Spark master is not running" -ForegroundColor Red
    Write-Host "Please start it with: docker-compose up -d spark-master spark-worker"
    exit 1
}
Write-Host "✅ Spark master is running" -ForegroundColor Green
Write-Host ""

# Note: Files are already accessible via shared volumes
Write-Host "📋 Files are accessible via shared volumes:" -ForegroundColor Cyan
Write-Host "   - FULL_STOCKS.csv -> /opt/spark/data/FULL_STOCKS.csv"
Write-Host "   - spark_questions.py -> /opt/spark/jobs/spark_questions.py"
Write-Host ""

# Submit Spark job
Write-Host "🚀 Submitting Spark job..." -ForegroundColor Yellow
Write-Host "==============================================" -ForegroundColor Cyan

docker-compose exec -T spark-master spark-submit `
    --master spark://spark-master:7077 `
    --executor-memory 2g `
    --driver-memory 1g `
    /opt/spark/jobs/spark_questions.py

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "==============================================" -ForegroundColor Green
    Write-Host "✅ Spark analysis completed successfully!" -ForegroundColor Green
    Write-Host "==============================================" -ForegroundColor Green
    Write-Host ""
    Write-Host "Results saved to: output\spark_results\" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "View results:" -ForegroundColor Cyan
    Write-Host "  dir output\spark_results\"
    Write-Host ""
    Write-Host "Access Spark UI:" -ForegroundColor Cyan
    Write-Host "  http://localhost:8080"
} else {
    Write-Host ""
    Write-Host "❌ Spark analysis failed" -ForegroundColor Red
    Write-Host "Check logs above for errors"
    exit 1
}

