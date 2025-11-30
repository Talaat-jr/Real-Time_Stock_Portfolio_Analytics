"""
Spark Data Analysis for Stock Portfolio
Performs distributed analytics on the cleaned stock trades data.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, count, max, min, stddev
from pyspark.sql.types import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session(app_name="StockPortfolioAnalysis"):
    """
    Create and configure Spark session.
    
    Args:
        app_name: Name of the Spark application
        
    Returns:
        SparkSession: Configured Spark session
    """
    logger.info("Creating Spark session...")
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("spark://spark-master:7077") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()
    
    logger.info("Spark session created successfully")
    return spark


def load_data(spark, data_path="output/FULL_STOCKS.csv"):
    """
    Load stock data into Spark DataFrame.
    
    Args:
        spark: SparkSession instance
        data_path: Path to the data file
        
    Returns:
        DataFrame: Spark DataFrame with loaded data
    """
    logger.info(f"Loading data from {data_path}...")
    
    df = spark.read.csv(
        data_path,
        header=True,
        inferSchema=True
    )
    
    logger.info(f"Data loaded: {df.count()} rows, {len(df.columns)} columns")
    return df


def analyze_stock_performance(df):
    """
    Analyze stock performance metrics.
    
    Args:
        df: Spark DataFrame
        
    Returns:
        DataFrame: Analysis results
    """
    logger.info("Analyzing stock performance...")
    
    stock_analysis = df.groupBy("stock_ticker") \
        .agg(
            count("transaction_id").alias("total_transactions"),
            sum("quantity").alias("total_volume"),
            avg("stock_price").alias("avg_price"),
            max("stock_price").alias("max_price"),
            min("stock_price").alias("min_price"),
            stddev("stock_price").alias("price_volatility"),
            sum("total_trade_amount").alias("total_value")
        ) \
        .orderBy(col("total_value").desc())
    
    logger.info("Stock performance analysis complete")
    return stock_analysis


def analyze_by_sector(df):
    """
    Analyze trading activity by sector.
    
    Args:
        df: Spark DataFrame
        
    Returns:
        DataFrame: Sector analysis results
    """
    logger.info("Analyzing by sector...")
    
    sector_analysis = df.groupBy("stock_sector") \
        .agg(
            count("transaction_id").alias("total_transactions"),
            sum("total_trade_amount").alias("total_value"),
            avg("stock_price").alias("avg_price"),
            count("stock_ticker").alias("unique_stocks")
        ) \
        .orderBy(col("total_value").desc())
    
    logger.info("Sector analysis complete")
    return sector_analysis


def analyze_by_customer_type(df):
    """
    Analyze trading patterns by customer account type.
    
    Args:
        df: Spark DataFrame
        
    Returns:
        DataFrame: Customer type analysis results
    """
    logger.info("Analyzing by customer type...")
    
    customer_analysis = df.groupBy("customer_account_type") \
        .agg(
            count("transaction_id").alias("total_transactions"),
            sum("total_trade_amount").alias("total_value"),
            avg("quantity").alias("avg_quantity"),
            count("customer_id").alias("unique_customers")
        ) \
        .orderBy(col("total_value").desc())
    
    logger.info("Customer type analysis complete")
    return customer_analysis


def analyze_temporal_patterns(df):
    """
    Analyze temporal trading patterns.
    
    Args:
        df: Spark DataFrame
        
    Returns:
        DataFrame: Temporal analysis results
    """
    logger.info("Analyzing temporal patterns...")
    
    temporal_analysis = df.groupBy("day_name", "is_weekend") \
        .agg(
            count("transaction_id").alias("total_transactions"),
            sum("total_trade_amount").alias("total_value"),
            avg("stock_price").alias("avg_price")
        ) \
        .orderBy(col("total_transactions").desc())
    
    logger.info("Temporal analysis complete")
    return temporal_analysis


def save_analysis_results(df, output_path, format="csv"):
    """
    Save analysis results to file.
    
    Args:
        df: Spark DataFrame to save
        output_path: Path to save results
        format: Output format (csv, parquet, json)
    """
    logger.info(f"Saving results to {output_path}...")
    
    if format == "csv":
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
    elif format == "parquet":
        df.write.mode("overwrite").parquet(output_path)
    elif format == "json":
        df.coalesce(1).write.mode("overwrite").json(output_path)
    
    logger.info("Results saved successfully")


def main():
    """
    Main execution function for Spark analysis.
    """
    try:
        logger.info("=" * 60)
        logger.info("Spark Stock Portfolio Analysis")
        logger.info("=" * 60)
        
        # Create Spark session
        spark = create_spark_session()
        
        # Load data
        df = load_data(spark, "/opt/spark/data/FULL_STOCKS.csv")
        
        # Show sample data
        logger.info("\nSample data:")
        df.show(5)
        
        # Perform analyses
        logger.info("\n--- Stock Performance Analysis ---")
        stock_perf = analyze_stock_performance(df)
        stock_perf.show(10)
        save_analysis_results(stock_perf, "/opt/spark/data/analysis_stock_performance")
        
        logger.info("\n--- Sector Analysis ---")
        sector_analysis = analyze_by_sector(df)
        sector_analysis.show()
        save_analysis_results(sector_analysis, "/opt/spark/data/analysis_by_sector")
        
        logger.info("\n--- Customer Type Analysis ---")
        customer_analysis = analyze_by_customer_type(df)
        customer_analysis.show()
        save_analysis_results(customer_analysis, "/opt/spark/data/analysis_by_customer_type")
        
        logger.info("\n--- Temporal Patterns Analysis ---")
        temporal_analysis = analyze_temporal_patterns(df)
        temporal_analysis.show()
        save_analysis_results(temporal_analysis, "/opt/spark/data/analysis_temporal_patterns")
        
        logger.info("\n" + "=" * 60)
        logger.info("Spark analysis completed successfully!")
        logger.info("=" * 60)
        
        # Stop Spark session
        spark.stop()
        
    except Exception as e:
        logger.error(f"Spark analysis failed: {e}", exc_info=True)
        raise


if __name__ == '__main__':
    main()
