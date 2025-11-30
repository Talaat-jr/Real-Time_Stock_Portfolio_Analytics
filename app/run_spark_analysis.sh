#!/bin/bash

# Run Spark Analysis Script
# This script helps execute Spark analysis on the FULL_STOCKS.csv

echo "=============================================="
echo "Spark Stock Portfolio Analysis Runner"
echo "=============================================="
echo ""

# Check if FULL_STOCKS.csv exists
if [ ! -f "output/FULL_STOCKS.csv" ]; then
    echo "❌ Error: output/FULL_STOCKS.csv not found"
    echo "Please run the main pipeline first: docker-compose up app"
    exit 1
fi

echo "✅ Found FULL_STOCKS.csv"
echo ""

# Create output directory for Spark results
mkdir -p output/spark_results
echo "✅ Created output/spark_results directory"
echo ""

# Check if Spark master is running
echo "🔍 Checking if Spark master is running..."
if ! docker ps | grep -q "spark-master"; then
    echo "❌ Spark master is not running"
    echo "Please start it with: docker-compose up -d spark-master spark-worker"
    exit 1
fi
echo "✅ Spark master is running"
echo ""

# Note: Files are already accessible via shared volumes
echo "📋 Files are accessible via shared volumes:"
echo "   - FULL_STOCKS.csv -> /opt/spark/data/FULL_STOCKS.csv"
echo "   - spark_questions.py -> /opt/spark/jobs/spark_questions.py"
echo ""

# Submit Spark job
echo "🚀 Submitting Spark job..."
echo "=============================================="
docker-compose exec -T spark-master spark-submit \
    --master spark://spark-master:7077 \
    --executor-memory 2g \
    --driver-memory 1g \
    /opt/spark/jobs/spark_questions.py

if [ $? -eq 0 ]; then
    echo ""
    echo "=============================================="
    echo "✅ Spark analysis completed successfully!"
    echo "=============================================="
    echo ""
    echo "Results saved to: output/spark_results/"
    echo ""
    echo "View results:"
    echo "  ls -la output/spark_results/"
    echo ""
    echo "Access Spark UI:"
    echo "  http://localhost:8080"
else
    echo ""
    echo "❌ Spark analysis failed"
    echo "Check logs above for errors"
    exit 1
fi
