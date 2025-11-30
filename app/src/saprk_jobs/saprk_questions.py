"""
Spark Data Analysis for Stock Portfolio - Milestone 2 Part 3
Performs distributed analytics on FULL_STOCKS.csv using Spark DataFrames and Spark SQL.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, avg, count, desc, when
from pyspark.sql.types import *
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session(app_name="StockPortfolioAnalysis"):
    """
    Create and configure Spark session connecting to spark-master.
    
    Args:
        app_name: Name of the Spark application
        
    Returns:
        SparkSession: Configured Spark session
    """
    logger.info("Creating Spark session...")
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("spark://spark-master:7077") \
        .getOrCreate()
    
    logger.info("Spark session created successfully")
    logger.info(f"Spark version: {spark.version}")
    
    return spark


def load_data(spark, data_path):
    """
    Read CSV file using Spark as a Spark DataFrame.
    
    Args:
        spark: SparkSession instance
        data_path: Path to FULL_STOCKS.csv
        
    Returns:
        DataFrame: Spark DataFrame with loaded data
    """
    logger.info(f"Loading data from {data_path}...")
    
    df = spark.read.csv(
        data_path,
        header=True,
        inferSchema=True
    )
    
    logger.info(f"Data loaded successfully")
    logger.info(f"Total rows: {df.count()}")
    logger.info(f"Total columns: {len(df.columns)}")
    
    return df


def show_sample_data(df, n=10):
    """
    Show the first N records to ensure data is correctly loaded.
    
    Args:
        df: Spark DataFrame
        n: Number of records to show
    """
    logger.info(f"\n{'='*80}")
    logger.info(f"SHOWING FIRST {n} RECORDS")
    logger.info(f"{'='*80}")
    df.show(n, truncate=False)
    
    logger.info("\nDataFrame Schema:")
    df.printSchema()


# ============================================================================
# 3.3.2 SPARK DATAFRAME FUNCTIONS ANALYSIS
# ============================================================================

def question_1_total_volume_by_ticker(df):
    """
    Q1: What is the total trading volume for each stock ticker?
    
    Args:
        df: Spark DataFrame
        
    Returns:
        DataFrame: Total volume per stock ticker
    """
    logger.info("\n" + "="*80)
    logger.info("Q1: Total Trading Volume for Each Stock Ticker")
    logger.info("="*80)
    
    result = df.groupBy("stock_ticker") \
        .agg(spark_sum("quantity").alias("total_volume")) \
        .orderBy(desc("total_volume"))
    
    logger.info("\nResults:")
    result.show(20, truncate=False)
    
    return result


def question_2_avg_price_by_sector(df):
    """
    Q2: What is the average stock price by sector?
    
    Args:
        df: Spark DataFrame
        
    Returns:
        DataFrame: Average stock price per sector
    """
    logger.info("\n" + "="*80)
    logger.info("Q2: Average Stock Price by Sector")
    logger.info("="*80)
    
    result = df.groupBy("stock_sector") \
        .agg(avg("stock_price").alias("average_price")) \
        .orderBy(desc("average_price"))
    
    logger.info("\nResults:")
    result.show(truncate=False)
    
    return result


def question_3_weekend_transactions(df):
    """
    Q3: How many buy vs sell transactions occurred on weekends?
    
    Args:
        df: Spark DataFrame
        
    Returns:
        DataFrame: Buy vs Sell transactions on weekends
    """
    logger.info("\n" + "="*80)
    logger.info("Q3: Buy vs Sell Transactions on Weekends")
    logger.info("="*80)
    
    # Filter for weekend transactions
    weekend_df = df.filter(col("is_weekend") == 1)
    
    result = weekend_df.groupBy("transaction_type") \
        .agg(count("transaction_id").alias("transaction_count")) \
        .orderBy(desc("transaction_count"))
    
    logger.info("\nResults:")
    result.show(truncate=False)
    
    # Additional stats
    total_weekend_transactions = weekend_df.count()
    logger.info(f"\nTotal weekend transactions: {total_weekend_transactions}")
    
    return result


def question_4_customers_more_than_10_transactions(df):
    """
    Q4: Which customers have made more than 10 transactions?
    
    Args:
        df: Spark DataFrame
        
    Returns:
        DataFrame: Customers with >10 transactions
    """
    logger.info("\n" + "="*80)
    logger.info("Q4: Customers with More Than 10 Transactions")
    logger.info("="*80)
    
    result = df.groupBy("customer_id") \
        .agg(count("transaction_id").alias("transaction_count")) \
        .filter(col("transaction_count") > 10) \
        .orderBy(desc("transaction_count"))
    
    logger.info(f"\nTotal customers with >10 transactions: {result.count()}")
    logger.info("\nTop 20 customers:")
    result.show(20, truncate=False)
    
    return result


def question_5_total_trade_by_day(df):
    """
    Q5: What is the total trade amount per day of the week, ordered from highest to lowest?
    
    Args:
        df: Spark DataFrame
        
    Returns:
        DataFrame: Total trade amount per day of week
    """
    logger.info("\n" + "="*80)
    logger.info("Q5: Total Trade Amount per Day of Week (Highest to Lowest)")
    logger.info("="*80)
    
    result = df.groupBy("day_name") \
        .agg(spark_sum("total_trade_amount").alias("total_trade_amount")) \
        .orderBy(desc("total_trade_amount"))
    
    logger.info("\nResults:")
    result.show(truncate=False)
    
    return result


# ============================================================================
# 3.3.3 SPARK SQL ANALYSIS
# ============================================================================

def sql_question_1_top_5_stocks_by_quantity(spark, df):
    """
    SQL Q1: What are the top 5 most traded stock tickers by total quantity?
    
    Args:
        spark: SparkSession instance
        df: Spark DataFrame
        
    Returns:
        DataFrame: Top 5 stocks by quantity
    """
    logger.info("\n" + "="*80)
    logger.info("SQL Q1: Top 5 Most Traded Stock Tickers by Total Quantity")
    logger.info("="*80)
    
    # Register DataFrame as temporary view
    df.createOrReplaceTempView("stocks")
    
    query = """
        SELECT 
            stock_ticker,
            SUM(quantity) as total_quantity
        FROM stocks
        GROUP BY stock_ticker
        ORDER BY total_quantity DESC
        LIMIT 5
    """
    
    result = spark.sql(query)
    
    logger.info("\nSQL Query:")
    logger.info(query)
    logger.info("\nResults:")
    result.show(truncate=False)
    
    return result


def sql_question_2_avg_trade_by_account_type(spark, df):
    """
    SQL Q2: What is the average trade amount by customer account type?
    
    Args:
        spark: SparkSession instance
        df: Spark DataFrame
        
    Returns:
        DataFrame: Average trade amount by account type
    """
    logger.info("\n" + "="*80)
    logger.info("SQL Q2: Average Trade Amount by Customer Account Type")
    logger.info("="*80)
    
    df.createOrReplaceTempView("stocks")
    
    query = """
        SELECT 
            customer_account_type,
            AVG(total_trade_amount) as avg_trade_amount,
            COUNT(*) as transaction_count
        FROM stocks
        GROUP BY customer_account_type
        ORDER BY avg_trade_amount DESC
    """
    
    result = spark.sql(query)
    
    logger.info("\nSQL Query:")
    logger.info(query)
    logger.info("\nResults:")
    result.show(truncate=False)
    
    return result


def sql_question_3_holiday_vs_non_holiday(spark, df):
    """
    SQL Q3: How many transactions occurred during holidays vs non-holidays?
    
    Args:
        spark: SparkSession instance
        df: Spark DataFrame
        
    Returns:
        DataFrame: Transactions during holidays vs non-holidays
    """
    logger.info("\n" + "="*80)
    logger.info("SQL Q3: Transactions During Holidays vs Non-Holidays")
    logger.info("="*80)
    
    df.createOrReplaceTempView("stocks")
    
    query = """
        SELECT 
            CASE 
                WHEN is_holiday = 1 THEN 'Holiday'
                ELSE 'Non-Holiday'
            END as period_type,
            COUNT(*) as transaction_count,
            SUM(total_trade_amount) as total_trade_amount
        FROM stocks
        GROUP BY is_holiday
        ORDER BY is_holiday DESC
    """
    
    result = spark.sql(query)
    
    logger.info("\nSQL Query:")
    logger.info(query)
    logger.info("\nResults:")
    result.show(truncate=False)
    
    return result


def sql_question_4_sectors_weekend_volume(spark, df):
    """
    SQL Q4: Which stock sectors had the highest total trading volume on weekends?
    
    Args:
        spark: SparkSession instance
        df: Spark DataFrame
        
    Returns:
        DataFrame: Sectors with highest weekend trading volume
    """
    logger.info("\n" + "="*80)
    logger.info("SQL Q4: Stock Sectors with Highest Weekend Trading Volume")
    logger.info("="*80)
    
    df.createOrReplaceTempView("stocks")
    
    query = """
        SELECT 
            stock_sector,
            SUM(quantity) as total_weekend_volume,
            COUNT(*) as weekend_transactions,
            SUM(total_trade_amount) as total_weekend_value
        FROM stocks
        WHERE is_weekend = 1
        GROUP BY stock_sector
        ORDER BY total_weekend_volume DESC
    """
    
    result = spark.sql(query)
    
    logger.info("\nSQL Query:")
    logger.info(query)
    logger.info("\nResults:")
    result.show(truncate=False)
    
    return result


def sql_question_5_buy_sell_by_liquidity(spark, df):
    """
    SQL Q5: What is the total buy vs sell amount for each stock liquidity tier?
    
    Args:
        spark: SparkSession instance
        df: Spark DataFrame
        
    Returns:
        DataFrame: Buy vs Sell amount by liquidity tier
    """
    logger.info("\n" + "="*80)
    logger.info("SQL Q5: Total Buy vs Sell Amount by Stock Liquidity Tier")
    logger.info("="*80)
    
    df.createOrReplaceTempView("stocks")
    
    query = """
        SELECT 
            stock_liquidity_tier,
            SUM(CASE WHEN transaction_type = 'BUY' THEN total_trade_amount ELSE 0 END) as total_buy_amount,
            SUM(CASE WHEN transaction_type = 'SELL' THEN total_trade_amount ELSE 0 END) as total_sell_amount,
            SUM(total_trade_amount) as total_amount,
            COUNT(CASE WHEN transaction_type = 'BUY' THEN 1 END) as buy_count,
            COUNT(CASE WHEN transaction_type = 'SELL' THEN 1 END) as sell_count
        FROM stocks
        GROUP BY stock_liquidity_tier
        ORDER BY total_amount DESC
    """
    
    result = spark.sql(query)
    
    logger.info("\nSQL Query:")
    logger.info(query)
    logger.info("\nResults:")
    result.show(truncate=False)
    
    return result


def save_results(result_df, output_path, format="csv"):
    """
    Save analysis results to file.
    
    Args:
        result_df: Spark DataFrame to save
        output_path: Path to save results
        format: Output format (csv, parquet, json)
    """
    logger.info(f"Saving results to {output_path}...")
    
    try:
        if format == "csv":
            result_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
        elif format == "parquet":
            result_df.write.mode("overwrite").parquet(output_path)
        elif format == "json":
            result_df.coalesce(1).write.mode("overwrite").json(output_path)
        
        logger.info("Results saved successfully")
    except Exception as e:
        logger.error(f"Error saving results: {e}")


def main():
    """
    Main execution function for Spark analysis.
    """
    try:
        logger.info("\n" + "="*80)
        logger.info("SPARK STOCK PORTFOLIO ANALYSIS - MILESTONE 2 PART 3")
        logger.info("="*80)
        
        # 3.3.1: Create Spark session
        spark = create_spark_session("StockPortfolioAnalysis")
        
        # Determine data path based on environment
        if os.path.exists('/opt/bitnami/spark/data/FULL_STOCKS.csv'):
            data_path = '/opt/bitnami/spark/data/FULL_STOCKS.csv'
        elif os.path.exists('output/FULL_STOCKS.csv'):
            data_path = 'output/FULL_STOCKS.csv'
        else:
            data_path = 'FULL_STOCKS.csv'
        
        # 3.3.1: Extract data - Read CSV as Spark DataFrame
        df = load_data(spark, data_path)
        
        # 3.3.1: Show first 10 records
        show_sample_data(df, n=10)
        
        # ====================================================================
        # 3.3.2: SPARK DATAFRAME FUNCTIONS ANALYSIS
        # ====================================================================
        
        logger.info("\n" + "="*80)
        logger.info("PART 3.3.2: SPARK DATAFRAME FUNCTIONS ANALYSIS")
        logger.info("="*80)
        
        # Q1: Total trading volume by ticker
        q1_result = question_1_total_volume_by_ticker(df)
        save_results(q1_result, "output/spark_results/q1_volume_by_ticker")
        
        # Q2: Average price by sector
        q2_result = question_2_avg_price_by_sector(df)
        save_results(q2_result, "output/spark_results/q2_avg_price_by_sector")
        
        # Q3: Weekend transactions (buy vs sell)
        q3_result = question_3_weekend_transactions(df)
        save_results(q3_result, "output/spark_results/q3_weekend_transactions")
        
        # Q4: Customers with >10 transactions
        q4_result = question_4_customers_more_than_10_transactions(df)
        save_results(q4_result, "output/spark_results/q4_active_customers")
        
        # Q5: Total trade amount by day of week
        q5_result = question_5_total_trade_by_day(df)
        save_results(q5_result, "output/spark_results/q5_trade_by_day")
        
        # ====================================================================
        # 3.3.3: SPARK SQL ANALYSIS
        # ====================================================================
        
        logger.info("\n" + "="*80)
        logger.info("PART 3.3.3: SPARK SQL ANALYSIS")
        logger.info("="*80)
        
        # SQL Q1: Top 5 stocks by quantity
        sql_q1_result = sql_question_1_top_5_stocks_by_quantity(spark, df)
        save_results(sql_q1_result, "output/spark_results/sql_q1_top_5_stocks")
        
        # SQL Q2: Average trade by account type
        sql_q2_result = sql_question_2_avg_trade_by_account_type(spark, df)
        save_results(sql_q2_result, "output/spark_results/sql_q2_avg_by_account")
        
        # SQL Q3: Holiday vs non-holiday transactions
        sql_q3_result = sql_question_3_holiday_vs_non_holiday(spark, df)
        save_results(sql_q3_result, "output/spark_results/sql_q3_holiday_comparison")
        
        # SQL Q4: Sectors with highest weekend volume
        sql_q4_result = sql_question_4_sectors_weekend_volume(spark, df)
        save_results(sql_q4_result, "output/spark_results/sql_q4_weekend_sectors")
        
        # SQL Q5: Buy vs sell by liquidity tier
        sql_q5_result = sql_question_5_buy_sell_by_liquidity(spark, df)
        save_results(sql_q5_result, "output/spark_results/sql_q5_liquidity_analysis")
        
        # ====================================================================
        # SUMMARY
        # ====================================================================
        
        logger.info("\n" + "="*80)
        logger.info("ANALYSIS SUMMARY")
        logger.info("="*80)
        logger.info("✅ All 5 Spark DataFrame questions completed")
        logger.info("✅ All 5 Spark SQL questions completed")
        logger.info("✅ Results saved to output/spark_results/")
        logger.info("="*80)
        
        # Stop Spark session
        spark.stop()
        logger.info("\nSpark session stopped successfully")
        
    except Exception as e:
        logger.error(f"Spark analysis failed: {e}", exc_info=True)
        raise


if __name__ == '__main__':
    main()
