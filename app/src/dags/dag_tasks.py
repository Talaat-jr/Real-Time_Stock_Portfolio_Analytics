"""
DAG Task Functions for Stock Portfolio Pipeline
Wraps original functions from app.py, db_utils.py, encoding_utils.py, kafka_utils.py
"""

import pandas as pd
import numpy as np
import os
import json
import logging
import sys
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
import pyspark.sql.functions as F

# Add src directory to Python path to import original modules
SRC_DIR = '/opt/airflow/src'
sys.path.insert(0, SRC_DIR)

DATA_DIR = '/opt/airflow/data'
OUTPUT_DIR = '/opt/airflow/output'
AGENTS_DIR = '/agents'
INTERMEDIATE_DIR = OUTPUT_DIR + '/intermediate'

# Import original functions from your implementation
from app import (
    read_file,
    handle_outliers,
    handle_missing_values,
    integrate_data,
    split_for_streaming,
    save_streaming_attributes,
    load_data as load_all_datasets
)
from db_utils import save_to_db, initialize_database
from encoding_utils import encode_data, save_lookup_tables, encode_single_record
from kafka_utils import create_consumer, consume_and_process

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Change working directory to where data and output folders are
os.chdir('/opt/airflow')

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def save_csv_file(df, filename):
    """
    Helper function: Save DataFrame to CSV in output directory.
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, filename)
    df.to_csv(output_path, index=False)
    logger.info(f"Saved {filename} with {len(df)} rows")
    return output_path

# =============================================================================
# STAGE 1: DATA CLEANING & INTEGRATION TASKS
# =============================================================================

def clean_missing_values_task(**context):
    """
    Task: Handle missing values in daily_trade_prices.csv
    Uses YOUR original handle_missing_values function from app.py
    """
    logger.info("=== TASK: Clean Missing Values ===")
    
    # Read daily trade prices using your original function
    df = read_file('daily_trade_prices.csv')
    logger.info(f"Loaded daily_trade_prices: {df.shape}")
    
    # Use YOUR original function
    cleaned_df = handle_missing_values(df, method='ffill')
    
    # Save cleaned data
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    cleaned_df.to_csv(os.path.join(OUTPUT_DIR, 'cleaned_daily_trade_prices.csv'), index=False)
    
    # Push to XCom for next tasks
    context['task_instance'].xcom_push(key='cleaned_prices_path', value=os.path.join(OUTPUT_DIR, 'cleaned_daily_trade_prices.csv'))
    
    logger.info("✓ Missing values cleaned successfully")


def detect_outliers_task(**context):
    """
    Task: Detect and handle outliers using YOUR original handle_outliers function.
    """
    logger.info("=== TASK: Detect and Handle Outliers ===")
    
    # Read cleaned daily trade prices
    cleaned_prices_path = context['task_instance'].xcom_pull(key='cleaned_prices_path', task_ids='data_cleaning_integration.clean_missing_values')
    df = pd.read_csv(cleaned_prices_path)
    
    # Also handle outliers in trades.csv using your original function
    trades_df = read_file('trades.csv')
    
    # Use YOUR original handle_outliers function
    df_clean = handle_outliers(df, method='cap', threshold=0.1)
    trades_clean = handle_outliers(trades_df, method='cap', threshold=0.2)
    
    # Save cleaned data
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df_clean.to_csv(os.path.join(OUTPUT_DIR, 'cleaned_daily_trade_prices.csv'), index=False)
    trades_clean.to_csv(os.path.join(OUTPUT_DIR, 'cleaned_trades.csv'), index=False)
    
    # Push to XCom
    context['task_instance'].xcom_push(key='cleaned_prices_final', value=os.path.join(OUTPUT_DIR, 'cleaned_daily_trade_prices.csv'))
    context['task_instance'].xcom_push(key='cleaned_trades', value=os.path.join(OUTPUT_DIR, 'cleaned_trades.csv'))
    
    logger.info("✓ Outliers detected and handled successfully")


def integrate_datasets_task(**context):
    """
    Task: Integrate all datasets starting from trades.csv.
    Merges customer, stock, date, and price data using the original integrate_data function.
    """
    logger.info("=== TASK: Integrate Datasets ===")
    
    # Load the cleaned files from previous tasks
    trades_path = context['task_instance'].xcom_pull(key='cleaned_trades', task_ids='data_cleaning_integration.detect_outliers')
    prices_path = context['task_instance'].xcom_pull(key='cleaned_prices_final', task_ids='data_cleaning_integration.detect_outliers')
    
    # Load datasets
    trades_df = pd.read_csv(trades_path)
    cleaned_daily_trade_prices_df = pd.read_csv(prices_path)
    dim_customer_df = read_file('dim_customer.csv')
    dim_stock_df = read_file('dim_stock.csv')
    dim_date_df = read_file('dim_date.csv')
    
    logger.info(f"Loaded datasets - Trades: {len(trades_df)}, Prices: {len(cleaned_daily_trade_prices_df)}")
    logger.info(f"  - Customers: {len(dim_customer_df)}, Stocks: {len(dim_stock_df)}, Dates: {len(dim_date_df)}")
    
    # Melt daily prices from wide to long format
    melted_daily_trade_prices_df = pd.melt(
        cleaned_daily_trade_prices_df, 
        id_vars=['date'], 
        var_name='stock_ticker', 
        value_name='stock_price'
    )
    melted_daily_trade_prices_df['date'] = pd.to_datetime(melted_daily_trade_prices_df['date'])
    logger.info("Daily prices melted to long format")
    
    # Start with trades as base
    merged_df = trades_df[[
        'transaction_id', 'timestamp', 'customer_id', 
        'stock_ticker', 'transaction_type', 'quantity', 'average_trade_size'
    ]].copy()
    merged_df['timestamp'] = pd.to_datetime(merged_df['timestamp'])
    logger.info(f"Base trades dataframe: {len(merged_df)} rows")
    
    # Merge stock prices based on date and ticker
    merged_df = merged_df.merge(
        melted_daily_trade_prices_df, 
        left_on=['timestamp', 'stock_ticker'], 
        right_on=['date', 'stock_ticker'], 
        how='left'
    )
    logger.info("Merged stock prices")
    
    # Calculate total trade amount
    merged_df['total_trade_amount'] = merged_df['stock_price'] * merged_df['quantity']
    logger.info("Calculated total trade amounts")
    
    # Merge customer account type
    merged_df = merged_df.merge(
        dim_customer_df[['customer_id', 'account_type']], 
        on='customer_id', 
        how='left'
    )
    logger.info("Merged customer account types")
    
    # Merge date features
    dim_date_df['date'] = pd.to_datetime(dim_date_df['date'])
    merged_df = merged_df.merge(
        dim_date_df[['date', 'day_name', 'is_weekend', 'is_holiday']], 
        left_on='timestamp', 
        right_on='date', 
        how='left'
    )
    logger.info("Merged date features")
    
    # Merge stock characteristics
    merged_df = merged_df.merge(
        dim_stock_df[['stock_ticker', 'liquidity_tier', 'sector', 'industry']], 
        on='stock_ticker', 
        how='left'
    )
    logger.info("Merged stock characteristics")
    
    # Rename columns to match target schema
    merged_df = merged_df.rename(columns={
        'account_type': 'customer_account_type',
        'liquidity_tier': 'stock_liquidity_tier',
        'sector': 'stock_sector',
        'industry': 'stock_industry'
    })
    
    # Drop unnecessary date columns from joins
    merged_df = merged_df.drop(columns=['date_x', 'date_y'], errors='ignore')
    
    logger.info(f"Integration complete: {len(merged_df)} rows, {len(merged_df.columns)} columns")
    
    # Save integrated data
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    integrated_path = os.path.join(OUTPUT_DIR, 'integrated_data.csv')
    merged_df.to_csv(integrated_path, index=False)
    
    # Push to XCom
    context['task_instance'].xcom_push(key='integrated_data', value=integrated_path)
    
    logger.info("✓ Datasets integrated successfully")


def load_to_postgres_task(**context):
    """
    Task: Load integrated/cleaned data into PostgreSQL warehouse.
    Uses the original save_to_db function from db_utils.
    """
    logger.info("=== TASK: Load to PostgreSQL ===")
    
    # Get integrated data from previous task
    integrated_path = context['task_instance'].xcom_pull(key='integrated_data', task_ids='data_cleaning_integration.integrate_datasets')
    
    # Read the integrated data
    df = pd.read_csv(integrated_path)
    logger.info(f"Loaded integrated data: {len(df)} rows, {len(df.columns)} columns")
    
    # Initialize database if needed
    initialize_database()
    logger.info("Database initialized")
    
    # Save to PostgreSQL using the original function
    table_name = 'stock_trades_integrated'
    save_to_db(df, table_name, if_exists='replace')
    
    logger.info(f"✓ Data loaded to PostgreSQL table '{table_name}'")
    
    # Push table name to XCom for downstream tasks
    context['task_instance'].xcom_push(key='postgres_table', value=table_name)


# =============================================================================
# STAGE 2: ENCODING & STREAM PREPARATION TASKS
# =============================================================================

def prepare_streaming_data_task(**context):
    """
    Task: Extract 5% random sample for streaming using YOUR original functions.
    """
    logger.info("=== TASK: Prepare Streaming Data ===")
    
    # Read integrated data
    integrated_path = context['task_instance'].xcom_pull(key='integrated_data', task_ids='data_cleaning_integration.integrate_datasets')
    df = pd.read_csv(integrated_path)
    
    # Use YOUR original split_for_streaming function
    main_df, stream_df = split_for_streaming(df, stream_percentage=0.05)
    
    # Save stream data
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    stream_df.to_csv(os.path.join(OUTPUT_DIR, 'stream.csv'), index=False)
    logger.info(f"Saved stream data: {len(stream_df)} rows")
    
    # Use YOUR original save_streaming_attributes function
    save_streaming_attributes(main_df)
    
    # Save main data for encoding
    main_df.to_csv(os.path.join(OUTPUT_DIR, 'main_data_for_encoding.csv'), index=False)
    
    # Push to XCom
    context['task_instance'].xcom_push(key='main_data', value=os.path.join(OUTPUT_DIR, 'main_data_for_encoding.csv'))
    context['task_instance'].xcom_push(key='stream_data', value=os.path.join(OUTPUT_DIR, 'stream.csv'))
    
    logger.info("✓ Streaming data prepared successfully")


def encode_categorical_data_task(**context):
    """
    Task: Apply encoding using YOUR original encode_data and save_lookup_tables functions.
    """
    logger.info("=== TASK: Encode Categorical Data ===")
    
    # Read main data
    main_path = context['task_instance'].xcom_pull(key='main_data', task_ids='encoding_stream_preparation.prepare_streaming_data')
    df = pd.read_csv(main_path)
    
    # Use YOUR original encode_data function
    encoded_df, lookup_tables = encode_data(df)
    
    # Use YOUR original save_lookup_tables function
    lookup_dir = os.path.join(OUTPUT_DIR, 'lookup_tables')
    save_lookup_tables(lookup_tables, output_dir=lookup_dir)
    
    # Save encoded main dataset
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    encoded_df.to_csv(os.path.join(OUTPUT_DIR, 'FULL_STOCKS.csv'), index=False)
    logger.info(f"Saved encoded data: {len(encoded_df)} rows")
    
    # Push to XCom
    context['task_instance'].xcom_push(key='encoded_data', value=os.path.join(OUTPUT_DIR, 'FULL_STOCKS.csv'))
    context['task_instance'].xcom_push(key='lookup_tables_dir', value=lookup_dir)
    
    logger.info("✓ Categorical data encoded successfully")


# =============================================================================
# STAGE 3: KAFKA STREAMING TASKS
# =============================================================================

def consume_and_process_stream_task(**context):
    """
    Task: Use YOUR original create_consumer and consume_and_process functions from kafka_utils.py
    """
    logger.info("=== TASK: Consume and Process Stream ===")
    
    # Get lookup tables directory
    lookup_dir = context['task_instance'].xcom_pull(key='lookup_tables_dir', task_ids='encoding_stream_preparation.encode_categorical_data')
    
    # Load lookup tables as dict (needed for your original function)
    lookup_tables = {}
    for filename in os.listdir(lookup_dir):
        if filename.startswith('lookup_') and filename.endswith('.csv'):
            col_name = filename.replace('lookup_', '').replace('.csv', '')
            lookup_df = pd.read_csv(os.path.join(lookup_dir, filename))
            # Convert to dict format expected by your encode_single_record function
            lookup_tables[col_name] = lookup_df
    
    # Get topic name
    topic_name = os.getenv('KAFKA_TOPIC_NAME', '55_23722_23342_Topic')
    
    try:
        # Count records before streaming
        full_stocks_path = os.path.join(OUTPUT_DIR, 'FULL_STOCKS.csv')
        before_df = pd.read_csv(full_stocks_path)
        records_before = len(before_df)
        logger.info(f"Records BEFORE streaming: {records_before}")
        
        # Use YOUR original create_consumer function
        consumer = create_consumer(topic_name, bootstrap_servers='kafka:9092')
        
        # Use YOUR original consume_and_process function
        streamed_records = consume_and_process(
            consumer,
            lookup_tables,
            output_file=full_stocks_path
        )
        
        # Count records after streaming
        after_df = pd.read_csv(full_stocks_path)
        records_after = len(after_df)
        logger.info(f"Records AFTER streaming: {records_after}")
        logger.info(f"Processed {streamed_records} records from Kafka stream")
        logger.info(f"Net increase: {records_after - records_before} records")
        
        # Verify we got all the streamed records
        if streamed_records > 0 and (records_after - records_before) != streamed_records:
            logger.warning(f"⚠ MISMATCH: Expected {streamed_records} new records, but got {records_after - records_before}")
        else:
            logger.info(f"✓ Successfully appended all {streamed_records} streamed records")
        
        # Push to XCom
        context['task_instance'].xcom_push(key='final_stocks', value=full_stocks_path)
        context['task_instance'].xcom_push(key='streamed_record_count', value=streamed_records)
        context['task_instance'].xcom_push(key='total_records', value=records_after)
        
        logger.info("✓ Stream consumed and processed successfully")
        
    except Exception as e:
        logger.warning(f"Kafka consumer error: {e}")
        logger.info("Using FULL_STOCKS as FINAL_STOCKS (no streaming)")
        
        # Fallback: FULL_STOCKS is already the final file
        context['task_instance'].xcom_push(key='final_stocks', value=os.path.join(OUTPUT_DIR, 'FULL_STOCKS.csv'))


def save_final_to_postgres_task(**context):
    """
    Task: Save FINAL_STOCKS.csv to PostgreSQL using YOUR original save_to_db function.
    """
    logger.info("=== TASK: Save Final Stocks to PostgreSQL ===")
    
    # Read FINAL_STOCKS
    final_path = context['task_instance'].xcom_pull(key='final_stocks', task_ids='kafka_streaming.consume_and_process_stream')
    df = pd.read_csv(final_path)
    
    # Use YOUR original save_to_db function
    table_name = 'final_stock_trades'
    save_to_db(df, table_name)
    
    logger.info(f"✓ Loaded {len(df)} rows to {table_name}")
    
    # Push to XCom
    context['task_instance'].xcom_push(key='final_table', value=table_name)


# =============================================================================
# STAGE 4: SPARK ANALYTICS TASKS
# =============================================================================

def create_spark_session(**context):
    """
    Helper function: Create and configure Spark session with proper settings.
    Based on best practices from spark_script.py
    """
    team_name = os.getenv('TEAM_NAME', 'team_name')
    
    spark = (
        SparkSession.builder
        .appName(f"M3_SPARK_APP_{team_name}")
        .master("spark://spark-master:7077")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.driver.host", "airflow-worker")  # Airflow worker container
        .config("spark.driver.port", "7078")
        .config("spark.blockManager.port", "7079")
        .config("spark.executor.memory", "1g")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.heartbeatInterval", "60s")
        .config("spark.network.timeout", "300s")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")  # Disable arrow for stability
        .config("spark.driver.maxResultSize", "512m")
        .config("spark.rpc.message.maxSize", "256")
        .config("spark.sql.adaptive.enabled", "true")  # Enable adaptive query execution
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.python.worker.reuse", "false")  # Prevent worker reuse issues
        .config("spark.storage.memoryFraction", "0.6")  # Better memory management
        .getOrCreate()
    )
    
    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def save_spark_result(result_df, query_name, table_name, engine):
    """
    Helper function: Save Spark DataFrame results to CSV and PostgreSQL.
    
    Args:
        result_df: Spark DataFrame with query results
        query_name: Name for the CSV output directory
        table_name: PostgreSQL table name (e.g., 'spark_analytics_q1')
        engine: SQLAlchemy engine for PostgreSQL connection
    """
    try:
        # Convert to Pandas with retry logic
        pandas_df = result_df.toPandas()
        
        # Save to CSV in output directory
        output_dir = os.path.join(OUTPUT_DIR, 'spark_results', query_name)
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, 'part-00000.csv')
        pandas_df.to_csv(output_path, index=False)
        
        # Save to PostgreSQL
        pandas_df.to_sql(table_name, engine, if_exists='replace', index=False)
        
        logger.info(f"  Saved {len(pandas_df)} rows to {output_path} and PostgreSQL table '{table_name}'")
        
        return pandas_df
    except Exception as e:
        logger.error(f"  Failed to save {query_name}: {e}")
        # Try to persist and collect again
        try:
            logger.info(f"  Retrying {query_name} with cache...")
            result_df.cache()
            pandas_df = result_df.toPandas()
            
            output_dir = os.path.join(OUTPUT_DIR, 'spark_results', query_name)
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, 'part-00000.csv')
            pandas_df.to_csv(output_path, index=False)
            
            pandas_df.to_sql(table_name, engine, if_exists='replace', index=False)
            logger.info(f"  Retry successful: Saved {len(pandas_df)} rows to {table_name}")
            return pandas_df
        except Exception as retry_error:
            logger.error(f"  Retry failed for {query_name}: {retry_error}")
            raise


def initialize_spark_session_task(**context):
    """
    Task: Connect to Spark master and create session named M3_SPARK_APP_TEAM_NAME.
    Test connection and verify Spark cluster availability.
    """
    logger.info("=== TASK: Initialize Spark Session ===")
    
    team_name = os.getenv('TEAM_NAME', 'team_name')
    
    try:
        # Create Spark session with improved configuration
        spark = create_spark_session(**context)
        
        logger.info(f"✓ Spark session created: M3_SPARK_APP_{team_name}")
        logger.info(f"  Master: {spark.sparkContext.master}")
        logger.info(f"  App ID: {spark.sparkContext.applicationId}")
        logger.info(f"  Spark Version: {spark.version}")
        
        # Test simple operation
        test_df = spark.createDataFrame([(1, "test")], ["id", "value"])
        count = test_df.count()
        logger.info(f"  Test operation successful: created test df with {count} row")
        
        # Stop session (will be recreated in next task)
        spark.stop()
        
        context['task_instance'].xcom_push(key='spark_app_name', value=f"M3_SPARK_APP_{team_name}")
        logger.info("✓ Spark initialization successful")
        
    except Exception as e:
        logger.error(f"Failed to initialize Spark session: {e}")
        raise


def run_spark_analytics_task(**context):
    """
    Task: Run comprehensive Spark DataFrame operations and Spark SQL queries.
    Based on spark_script.py with improved structure and error handling.
    Saves results to both CSV files and PostgreSQL tables.
    """
    logger.info("=== TASK: Run Spark Analytics ===")
    
    try:
        # Create Spark session
        spark = create_spark_session(**context)
        
        # Create PostgreSQL connection for saving results
        from sqlalchemy import create_engine
        conn_string = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
        engine = create_engine(conn_string)
        logger.info("PostgreSQL connection established for analytics results")
        
        # Read FULL_STOCKS.csv
        final_path = context['task_instance'].xcom_pull(
            key='final_stocks', 
            task_ids='kafka_streaming.consume_and_process_stream'
        )
        
        logger.info(f"Loading data from: {final_path}")
        
        # Load lookup tables for decoding
        lookup_dir = context['task_instance'].xcom_pull(
            key='lookup_tables_dir', 
            task_ids='encoding_stream_preparation.encode_categorical_data'
        )
        
        # Read CSV using Pandas first (from Airflow worker), then convert to Spark DataFrame
        # This avoids file distribution issues with Spark executors
        pandas_df = pd.read_csv(final_path)
        logger.info(f"Loaded {len(pandas_df)} rows via Pandas")
        
        # Decode categorical columns back to original values
        logger.info("Decoding categorical columns using lookup tables...")
        reverse_lookups = {}
        for filename in os.listdir(lookup_dir):
            if filename.startswith('lookup_') and filename != 'lookup_all_encodings.csv':
                col_name = filename.replace('lookup_', '').replace('.csv', '')
                lookup_df = pd.read_csv(os.path.join(lookup_dir, filename))
                # Create reverse mapping: Encoded_Value -> Original_Value (note capitalization)
                if 'Encoded_Value' in lookup_df.columns and 'Original_Value' in lookup_df.columns:
                    reverse_lookups[col_name] = dict(zip(lookup_df['Encoded_Value'], lookup_df['Original_Value']))
                else:
                    logger.warning(f"  Skipping {filename} - missing required columns")
        
        # Apply decoding to each categorical column
        for col, reverse_map in reverse_lookups.items():
            if col in pandas_df.columns:
                pandas_df[col] = pandas_df[col].map(reverse_map).fillna(pandas_df[col])
                logger.info(f"  Decoded column: {col}")
        
        logger.info(f"Decoding complete - {len(reverse_lookups)} columns decoded")
        
        # Convert to Spark DataFrame
        df = spark.createDataFrame(pandas_df)
        logger.info(f"Converted to Spark DataFrame with {len(df.columns)} columns")
        
        total_rows = df.count()
        total_cols = len(df.columns)
        logger.info(f"Loaded {total_rows} rows and {total_cols} columns into Spark DataFrame")
        
        # Reverse natural log of stock_price into a new column (if stock_price exists)
        if 'stock_price' in df.columns:
            df = df.withColumn("stock_price_original", F.exp(F.col("stock_price")))
            logger.info("Created stock_price_original column by reversing log transformation")
        
        # Show first 10 rows for verification
        logger.info("First 10 rows of FULL_STOCKS:")
        df.show(10, truncate=False)
        
        # Register as temp view for SQL queries
        df.createOrReplaceTempView("stocks")
        logger.info("Registered 'stocks' temporary view for SQL queries")
        
        # =================================================================
        # DataFrame Operations (Section 3.3.2)
        # =================================================================
        logger.info("=== Running DataFrame Operations (3.3.2) ===")
        
        # Q1: What is the total trading volume for each stock ticker?
        logger.info("Q1: Total trading volume for each stock ticker")
        q1_result = (df.groupBy("stock_ticker")
                      .sum("quantity")
                      .withColumnRenamed("sum(quantity)", "total_volume")
                      .orderBy(F.col("total_volume").desc()))
        q1_result.show(20, truncate=False)
        save_spark_result(q1_result, 'q1_volume_by_ticker', 'spark_analytics_q1', engine)
        
        # Q2: What is the average stock price by sector?
        logger.info("Q2: Average stock price by sector")
        sector_cols = [c for c in df.columns if c.startswith("sector_")]
        
        if sector_cols:
            # For one-hot encoded sectors, compute avg for each
            sector_results = []
            for col in sector_cols:
                try:
                    sector_name = col.replace("sector_", "")
                    # Use first() instead of collect() for safer single-value retrieval
                    avg_price_row = df.filter(F.col(col) == 1).agg(
                        F.avg("stock_price").alias("stock_price_logged")
                    ).first()
                    
                    if avg_price_row and avg_price_row[0] is not None:
                        avg_price_logged = avg_price_row[0]
                        
                        # Also compute original price average if available
                        if 'stock_price_original' in df.columns:
                            avg_price_orig_row = df.filter(F.col(col) == 1).agg(
                                F.avg("stock_price_original")
                            ).first()
                            avg_price_orig = avg_price_orig_row[0] if avg_price_orig_row else None
                            logger.info(f"  {sector_name}: {avg_price_orig:.2f} (logged: {avg_price_logged:.4f})")
                            sector_results.append((sector_name, avg_price_orig, avg_price_logged))
                        else:
                            logger.info(f"  {sector_name}: {avg_price_logged:.4f}")
                            sector_results.append((sector_name, None, avg_price_logged))
                except Exception as e:
                    logger.warning(f"Failed to compute average for sector {col}: {e}")
            
            # Create DataFrame from results
            if sector_results:
                q2_result = spark.createDataFrame(
                    sector_results, 
                    ["sector", "avg_price_original", "avg_price_logged"]
                )
                save_spark_result(q2_result, 'q2_avg_price_by_sector', 'spark_analytics_q2', engine)
        else:
            # Fallback for non-one-hot encoded data
            q2_result = df.groupBy("stock_sector").avg("stock_price")
            q2_result.show(20, truncate=False)
            save_spark_result(q2_result, 'q2_avg_price_by_sector', 'spark_analytics_q2', engine)
        
        # Q3: How many buy vs sell transactions occurred on weekends?
        logger.info("Q3: Buy vs Sell transactions on weekends")
        try:
            q3_result = (df.filter(F.col("is_weekend") == 1)
                          .groupBy("transaction_type")
                          .count()
                          .orderBy("transaction_type"))
            
            # Check if result has data before collecting
            q3_count = q3_result.count()
            if q3_count > 0:
                q3_result.show()
                
                # Pretty formatting - safer approach using first() with null checks
                buy_row = q3_result.filter("transaction_type = 'BUY'").select("count").first()
                sell_row = q3_result.filter("transaction_type = 'SELL'").select("count").first()
                
                buy_count = buy_row[0] if buy_row else 0
                sell_count = sell_row[0] if sell_row else 0
                
                logger.info(f"  BUY on weekends:  {buy_count}")
                logger.info(f"  SELL on weekends: {sell_count}")
            else:
                logger.info("  No weekend transactions found")
                buy_count = sell_count = 0
            
            save_spark_result(q3_result, 'q3_weekend_transactions', 'spark_analytics_q3', engine)
        except Exception as e:
            logger.warning(f"Q3 query failed: {e}. Continuing with pipeline.")
        
        # Q4: Which customers have made more than 10 transactions?
        logger.info("Q4: Customers with more than 10 transactions")
        q4_result = (df.groupBy("customer_id")
                      .count()
                      .filter(F.col("count") > 10)
                      .orderBy(F.col("count").desc()))
        
        q4_count = q4_result.count()
        if q4_count == 0:
            logger.info("  No customers with more than 10 transactions.")
        else:
            logger.info(f"  Found {q4_count} customers with more than 10 transactions")
            q4_result.show(q4_count, truncate=False)
        
        save_spark_result(q4_result, 'q4_active_customers', 'spark_analytics_q4', engine)
        
        # Q5: What is the total trade amount per day of the week?
        logger.info("Q5: Total trade amount per day of the week")
        df_with_day = df.withColumn("day_of_week", F.date_format("timestamp", "EEEE"))
        
        q5_result = (df_with_day.groupBy("day_of_week")
                      .agg(F.sum("total_trade_amount").alias("total_trade_amount"))
                      .orderBy(F.col("total_trade_amount").desc()))
        q5_result.show(10, truncate=False)
        save_spark_result(q5_result, 'q5_trade_by_day', 'spark_analytics_q5', engine)
        
        # =================================================================
        # Spark SQL Queries (Section 3.3.3)
        # =================================================================
        logger.info("=== Running Spark SQL Queries (3.3.3) ===")
        
        # SQL Q1: What are the top 5 most traded stock tickers by total quantity?
        logger.info("SQL Q1: Top 5 most traded stock tickers")
        try:
            sql_q1_result = spark.sql("""
                SELECT stock_ticker,
                    SUM(quantity) AS total_quantity
                FROM stocks
                GROUP BY stock_ticker
                ORDER BY total_quantity DESC
                LIMIT 5
            """)
            sql_q1_result.show(truncate=False)
            save_spark_result(sql_q1_result, 'sql_q1_top_5_stocks', 'spark_sql_q1', engine)
        except Exception as e:
            logger.warning(f"SQL Q1 query failed: {e}. Continuing with pipeline.")
        
        # SQL Q2: What is the average trade amount by customer account type?
        logger.info("SQL Q2: Average trade amount by account type")
        try:
            sql_q2_result = spark.sql("""
                SELECT customer_account_type,
                    AVG(total_trade_amount) AS avg_trade_amount
                FROM stocks
                GROUP BY customer_account_type
            """)
            sql_q2_result.show(truncate=False)
            save_spark_result(sql_q2_result, 'sql_q2_avg_by_account', 'spark_sql_q2', engine)
        except Exception as e:
            logger.warning(f"SQL Q2 query failed: {e}. Continuing with pipeline.")
        
        # SQL Q3: How many transactions occurred during holidays vs non-holidays?
        logger.info("SQL Q3: Transactions during holidays vs non-holidays")
        try:
            sql_q3_result = spark.sql("""
                SELECT is_holiday,
                    COUNT(*) AS transactions_count
                FROM stocks
                GROUP BY is_holiday
            """)
            sql_q3_result.show(truncate=False)
            save_spark_result(sql_q3_result, 'sql_q3_holiday_comparison', 'spark_sql_q3', engine)
        except Exception as e:
            logger.warning(f"SQL Q3 query failed: {e}. Continuing with pipeline.")
        
        # SQL Q4: Which stock sectors had the highest total trading volume on weekends?
        logger.info("SQL Q4: Weekend trading volume by sector")
        try:
            sql_q4_result = spark.sql("""
                SELECT stock_sector,
                    SUM(quantity) AS total_volume
                FROM stocks
                WHERE is_weekend = 1
                GROUP BY stock_sector
                ORDER BY total_volume DESC
            """)
            
            # Check if result has data before showing/saving
            q4_count = sql_q4_result.count()
            if q4_count > 0:
                sql_q4_result.show(truncate=False)
                save_spark_result(sql_q4_result, 'sql_q4_weekend_sectors', 'spark_sql_q4', engine)
            else:
                logger.info("  No weekend trading data found. Skipping Q4.")
                # Save empty result to maintain consistency
                pandas_df = sql_q4_result.toPandas()
                output_dir = os.path.join(OUTPUT_DIR, 'spark_results', 'sql_q4_weekend_sectors')
                os.makedirs(output_dir, exist_ok=True)
                pandas_df.to_csv(os.path.join(output_dir, 'part-00000.csv'), index=False)
        except Exception as e:
            logger.warning(f"SQL Q4 query failed: {e}. Continuing with pipeline.")
        
        # SQL Q5: What is the total buy vs sell amount for each stock liquidity tier?
        logger.info("SQL Q5: Buy vs Sell amount by liquidity tier")
        try:
            sql_q5_result = spark.sql("""
                SELECT stock_liquidity_tier,
                    SUM(CASE WHEN transaction_type = 'BUY' THEN total_trade_amount ELSE 0 END) AS buy_amount,
                    SUM(CASE WHEN transaction_type = 'SELL' THEN total_trade_amount ELSE 0 END) AS sell_amount
                FROM stocks
                GROUP BY stock_liquidity_tier
                ORDER BY stock_liquidity_tier
            """)
            sql_q5_result.show(truncate=False)
            save_spark_result(sql_q5_result, 'sql_q5_liquidity_analysis', 'spark_sql_q5', engine)
        except Exception as e:
            logger.warning(f"SQL Q5 query failed: {e}. Continuing with pipeline.")
        
        logger.info("✓ Spark analytics completed successfully")
        
        # Push results location to XCom
        context['task_instance'].xcom_push(
            key='spark_results_dir', 
            value=os.path.join(OUTPUT_DIR, 'spark_results')
        )
        
    except Exception as e:
        logger.error(f"Spark analytics failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise
    finally:
        # Always stop Spark session to free resources
        try:
            if 'spark' in locals():
                # Force cleanup of any cached data before stopping
                try:
                    spark.catalog.clearCache()
                except:
                    pass
                
                # Stop with error suppression for connection issues
                import time
                try:
                    spark.stop()
                    logger.info("Spark session stopped cleanly")
                except Exception as stop_ex:
                    # If connection already closed, that's fine
                    logger.warning(f"Spark session stop encountered error (may be already closed): {stop_ex}")
                    
                # Give time for cleanup
                time.sleep(2)
        except Exception as stop_error:
            logger.warning(f"Error during Spark session cleanup: {stop_error}")


# =============================================================================
# STAGE 5: DATA VISUALIZATION TASKS
# =============================================================================

def prepare_visualization_task(**context):
    """
    Task: Prepare columns and aggregations for visualization.
    Revert encoding transformations and create aggregated views for dashboard.
    """
    logger.info("=== TASK: Prepare Visualization Data ===")
    
    try:
        # Read FINAL_STOCKS (encoded data from Kafka stream)
        final_path = context['task_instance'].xcom_pull(key='final_stocks', task_ids='kafka_streaming.consume_and_process_stream')
        if not final_path or not os.path.exists(final_path):
            logger.warning("FINAL_STOCKS not available, using integrated_data instead")
            final_path = os.path.join(OUTPUT_DIR, 'integrated_data.csv')
        
        df = pd.read_csv(final_path)
        logger.info(f"Loaded data: {df.shape}")
        
        # Load lookup tables for decoding
        lookup_dir = context['task_instance'].xcom_pull(key='lookup_tables_dir', task_ids='encoding_stream_preparation.encode_categorical_data')
        if not lookup_dir:
            lookup_dir = os.path.join(OUTPUT_DIR, 'lookup_tables')
        
        # Load all encodings lookup table
        all_encodings_path = os.path.join(lookup_dir, 'lookup_all_encodings.csv')
        if os.path.exists(all_encodings_path):
            all_encodings_df = pd.read_csv(all_encodings_path)
            
            # Create reverse lookup dictionaries grouped by column
            reverse_lookups = {}
            for col_name in all_encodings_df['Column_Name'].unique():
                col_mappings = all_encodings_df[all_encodings_df['Column_Name'] == col_name]
                reverse_lookups[col_name] = dict(zip(col_mappings['Encoded_Value'], col_mappings['Original_Value']))
            
            # Decode categorical columns for visualization
            decoded_df = df.copy()
            for col, reverse_map in reverse_lookups.items():
                if col in decoded_df.columns:
                    decoded_df[col] = decoded_df[col].map(reverse_map).fillna(decoded_df[col])
                    logger.info(f"Decoded column: {col}")
        else:
            # If no encoding was done, use original data
            logger.warning("No encoding lookup found, using original data")
            decoded_df = df.copy()
        
        # Ensure timestamp is datetime
        if 'timestamp' in decoded_df.columns:
            decoded_df['timestamp'] = pd.to_datetime(decoded_df['timestamp'])
            decoded_df['date'] = decoded_df['timestamp'].dt.date
            decoded_df['year'] = decoded_df['timestamp'].dt.year
            decoded_df['month'] = decoded_df['timestamp'].dt.month
            decoded_df['month_name'] = decoded_df['timestamp'].dt.strftime('%B')
        
        # Save fully decoded data for visualization
        decoded_path = save_csv_file(decoded_df, 'FINAL_STOCKS_DECODED.csv')
        logger.info(f"Saved decoded data: {decoded_path}")
        
        # Create aggregated views for dashboard performance
        
        # 1. Volume by Ticker aggregation
        volume_by_ticker = decoded_df.groupby('stock_ticker').agg({
            'quantity': 'sum',
            'total_trade_amount': 'sum',
            'transaction_id': 'count'
        }).reset_index()
        volume_by_ticker.columns = ['stock_ticker', 'total_volume', 'total_amount', 'transaction_count']
        save_csv_file(volume_by_ticker, 'agg_volume_by_ticker.csv')
        
        # 2. Price trends by Sector
        price_by_sector = decoded_df.groupby(['stock_sector', 'timestamp']).agg({
            'stock_price': 'mean',
            'total_trade_amount': 'sum'
        }).reset_index()
        save_csv_file(price_by_sector, 'agg_price_by_sector.csv')
        
        # 3. Buy vs Sell summary
        buy_sell_summary = decoded_df.groupby('transaction_type').agg({
            'quantity': 'sum',
            'total_trade_amount': 'sum',
            'transaction_id': 'count'
        }).reset_index()
        buy_sell_summary.columns = ['transaction_type', 'total_quantity', 'total_amount', 'transaction_count']
        save_csv_file(buy_sell_summary, 'agg_buy_vs_sell.csv')
        
        # 4. Trading by Day of Week
        trading_by_day = decoded_df.groupby('day_name').agg({
            'quantity': 'sum',
            'total_trade_amount': 'sum',
            'transaction_id': 'count'
        }).reset_index()
        trading_by_day.columns = ['day_name', 'total_volume', 'total_amount', 'transaction_count']
        save_csv_file(trading_by_day, 'agg_trading_by_day.csv')
        
        # 5. Customer distribution
        customer_distribution = decoded_df.groupby('customer_id').agg({
            'total_trade_amount': 'sum',
            'transaction_id': 'count',
            'customer_account_type': 'first'
        }).reset_index()
        customer_distribution.columns = ['customer_id', 'total_amount', 'transaction_count', 'account_type']
        customer_distribution = customer_distribution.sort_values('total_amount', ascending=False)
        save_csv_file(customer_distribution, 'agg_customer_distribution.csv')
        
        # 6. Top 10 Customers
        top_customers = customer_distribution.head(10)
        save_csv_file(top_customers, 'agg_top_10_customers.csv')
        
        # 7. Holiday vs Non-Holiday
        holiday_comparison = decoded_df.groupby('is_holiday').agg({
            'quantity': 'sum',
            'total_trade_amount': 'sum',
            'transaction_id': 'count'
        }).reset_index()
        holiday_comparison['is_holiday'] = holiday_comparison['is_holiday'].map({False: 'Non-Holiday', True: 'Holiday'})
        save_csv_file(holiday_comparison, 'agg_holiday_comparison.csv')
        
        # 8. Sector comparison
        sector_comparison = decoded_df.groupby('stock_sector').agg({
            'total_trade_amount': ['sum', 'mean'],
            'quantity': 'sum',
            'transaction_id': 'count'
        }).reset_index()
        sector_comparison.columns = ['stock_sector', 'total_amount', 'avg_amount', 'total_volume', 'transaction_count']
        save_csv_file(sector_comparison, 'agg_sector_comparison.csv')
        
        # 9. Liquidity tier analysis
        liquidity_analysis = decoded_df.groupby(['stock_liquidity_tier', 'timestamp']).agg({
            'total_trade_amount': 'sum',
            'quantity': 'sum'
        }).reset_index()
        save_csv_file(liquidity_analysis, 'agg_liquidity_analysis.csv')
        
        # Save metadata about the visualization data
        metadata = {
            'total_transactions': len(decoded_df),
            'date_range': {
                'start': str(decoded_df['timestamp'].min()) if 'timestamp' in decoded_df.columns else 'N/A',
                'end': str(decoded_df['timestamp'].max()) if 'timestamp' in decoded_df.columns else 'N/A'
            },
            'unique_stocks': int(decoded_df['stock_ticker'].nunique()) if 'stock_ticker' in decoded_df.columns else 0,
            'unique_customers': int(decoded_df['customer_id'].nunique()) if 'customer_id' in decoded_df.columns else 0,
            'total_volume': float(decoded_df['quantity'].sum()) if 'quantity' in decoded_df.columns else 0,
            'total_trade_amount': float(decoded_df['total_trade_amount'].sum()) if 'total_trade_amount' in decoded_df.columns else 0
        }
        
        import json
        metadata_path = os.path.join(OUTPUT_DIR, 'visualization_metadata.json')
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        # Push to XCom
        context['task_instance'].xcom_push(key='viz_data', value=decoded_path)
        context['task_instance'].xcom_push(key='viz_metadata', value=metadata_path)
        
        logger.info("✓ Visualization data prepared successfully")
        logger.info(f"  - Total transactions: {metadata['total_transactions']}")
        logger.info(f"  - Unique stocks: {metadata['unique_stocks']}")
        logger.info(f"  - Unique customers: {metadata['unique_customers']}")
        
    except Exception as e:
        logger.error(f"Error in prepare_visualization_task: {str(e)}")
        raise


def start_streamlit_dashboard_task(**context):
    """
    Task: Start Streamlit dashboard after data is prepared.
    Kills existing Streamlit processes and starts a new one.
    """
    logger.info("=== TASK: Start Streamlit Dashboard ===")
    
    import subprocess
    import signal
    
    try:
        # Kill any existing Streamlit processes
        logger.info("Checking for existing Streamlit processes...")
        try:
            subprocess.run(
                ['pkill', '-f', 'streamlit run.*dashboard.py'],
                timeout=5
            )
            logger.info("Killed existing Streamlit processes")
            time.sleep(2)
        except subprocess.TimeoutExpired:
            logger.warning("Timeout while killing processes")
        except Exception as e:
            logger.info(f"No existing processes to kill: {e}")
        
        # Start Streamlit in background
        logger.info("Starting Streamlit dashboard...")
        dashboard_path = '/opt/airflow/src/dashboard.py'
        log_file = '/opt/airflow/logs/streamlit.log'
        
        # Open log file
        log_handle = open(log_file, 'w')
        
        # Start streamlit process
        process = subprocess.Popen(
            [
                'streamlit', 'run', dashboard_path,
                '--server.port', '8501',
                '--server.address', '0.0.0.0',
                '--server.headless', 'true',
                '--server.enableCORS', 'false',
                '--server.enableXsrfProtection', 'false'
            ],
            stdout=log_handle,
            stderr=log_handle,
            start_new_session=True  # Detach from parent process
        )
        
        logger.info(f"Streamlit process started with PID: {process.pid}")
        
        # Wait a bit for startup
        time.sleep(8)
        
        # Verify it's running
        result = subprocess.run(
            ['pgrep', '-f', 'streamlit run.*dashboard.py'],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0 and result.stdout.strip():
            pids = result.stdout.strip().split('\n')
            logger.info(f"✓ Streamlit dashboard started successfully!")
            logger.info(f"  PIDs: {', '.join(pids)}")
            logger.info(f"  Port: 8501")
            logger.info(f"  Access at: http://localhost:8501")
            logger.info(f"  Logs: {log_file}")
            
            # Store PID in XCom for potential cleanup
            context['task_instance'].xcom_push(key='streamlit_pids', value=pids)
            
            return True
        else:
            logger.error("✗ Failed to start Streamlit dashboard")
            logger.error(f"Check logs at: {log_file}")
            
            # Try to read last few lines of log
            try:
                with open(log_file, 'r') as f:
                    lines = f.readlines()
                    if lines:
                        logger.error("Last log lines:")
                        for line in lines[-10:]:
                            logger.error(f"  {line.strip()}")
            except:
                pass
            
            raise Exception("Streamlit failed to start")
            
    except Exception as e:
        logger.error(f"Error starting Streamlit: {str(e)}")
        raise


# =============================================================================
# STAGE 6: AI AGENT QUERY PIPELINE TASK
# =============================================================================

def process_with_ai_agent_task(**context):
    """
    Task: Process natural language query using AI agent and generate SQL.
    Reads from user_query.txt and appends to AGENT_LOGS.JSON.
    """
    logger.info("=== TASK: Process with AI Agent ===")
    
    # Read user query
    query_file = os.path.join(AGENTS_DIR, 'user_query.txt')
    
    # Create default query if file doesn't exist
    if not os.path.exists(query_file):
        os.makedirs(AGENTS_DIR, exist_ok=True)
        with open(query_file, 'w') as f:
            f.write("What was the total trading volume for technology stocks last month?")
    
    with open(query_file, 'r') as f:
        user_query = f.read().strip()
    
    logger.info(f"User query: {user_query}")
    
    # Get CSV columns
    final_path = context['task_instance'].xcom_pull(key='final_stocks', task_ids='kafka_streaming.consume_and_process_stream')
    df = pd.read_csv(final_path)
    columns = df.columns.tolist()
    columns_str = ", ".join(columns)
    
    # Create AI agent prompt
    system_prompt = f"""You are an SQL query generator. Given a natural language question and CSV column names, generate a valid SQL query.
    
Available columns: {columns_str}

Generate ONLY the SQL query without any explanation. The table name is 'stocks'."""
    
    # Simple AI agent (using placeholder - replace with actual LLM)
    # For demonstration, using a rule-based approach
    # In production, replace with OpenAI, HuggingFace, or Ollama
    
    generated_sql = generate_sql_query(user_query, columns)
    
    # Create agent log entry
    log_entry = {
        "timestamp": datetime.now().isoformat(),
        "user_query": user_query,
        "columns_available": columns,
        "generated_sql": generated_sql,
        "status": "success"
    }
    
    # Append to AGENT_LOGS.JSON
    logs_file = os.path.join(AGENTS_DIR, 'AGENT_LOGS.JSON')
    
    if os.path.exists(logs_file):
        with open(logs_file, 'r') as f:
            logs = json.load(f)
    else:
        logs = []
    
    logs.append(log_entry)
    
    with open(logs_file, 'w') as f:
        json.dump(logs, f, indent=2)
    
    logger.info(f"✓ AI Agent processed query successfully")
    logger.info(f"  Generated SQL: {generated_sql}")


def generate_sql_query(user_query, columns):
    """
    Simple rule-based SQL generator (replace with actual LLM in production).
    """
    query_lower = user_query.lower()
    
    # Pattern matching for common queries
    if 'total' in query_lower and 'volume' in query_lower:
        if 'technology' in query_lower or 'tech' in query_lower:
            return "SELECT SUM(quantity) as total_volume FROM stocks WHERE stock_sector = 'Technology'"
        else:
            return "SELECT stock_ticker, SUM(quantity) as total_volume FROM stocks GROUP BY stock_ticker"
    
    elif 'average' in query_lower or 'avg' in query_lower:
        if 'price' in query_lower:
            return "SELECT stock_sector, AVG(stock_price) as avg_price FROM stocks GROUP BY stock_sector"
        elif 'trade' in query_lower or 'amount' in query_lower:
            return "SELECT customer_account_type, AVG(total_trade_amount) as avg_amount FROM stocks GROUP BY customer_account_type"
    
    elif 'top' in query_lower:
        if 'customer' in query_lower:
            return "SELECT customer_id, SUM(total_trade_amount) as total_trades FROM stocks GROUP BY customer_id ORDER BY total_trades DESC LIMIT 10"
        elif 'stock' in query_lower:
            return "SELECT stock_ticker, SUM(quantity) as total_volume FROM stocks GROUP BY stock_ticker ORDER BY total_volume DESC LIMIT 10"
    
    # Default query
    return "SELECT * FROM stocks LIMIT 100"
