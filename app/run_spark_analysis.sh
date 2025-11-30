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

# Copy FULL_STOCKS.csv to Spark master container
echo "📋 Copying FULL_STOCKS.csv to Spark master..."
docker cp output/FULL_STOCKS.csv stock_spark_master:/opt/bitnami/spark/data/
if [ $? -eq 0 ]; then
    echo "✅ File copied successfully"
else
    echo "❌ Failed to copy file. Is Spark master running?"
    exit 1
fi
echo ""

# Copy Spark analysis script
echo "📋 Copying spark_analysis.py to Spark master..."
docker cp spark_jobs/spark_analysis.py stock_spark_master:/opt/bitnami/spark/jobs/
if [ $? -eq 0 ]; then
    echo "✅ Script copied successfully"
else
    echo "❌ Failed to copy script"
    exit 1
fi
echo ""

# Submit Spark job
echo "🚀 Submitting Spark job..."
echo "=============================================="
docker-compose exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    --executor-memory 2g \
    --driver-memory 1g \
    /opt/bitnami/spark/jobs/spark_analysis.py

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
