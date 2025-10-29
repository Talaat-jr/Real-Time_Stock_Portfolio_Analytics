import pandas as pd
import numpy as np
import os
from db_utils import save_to_db, initialize_database
import logging
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global dataframes
dim_customer_df = None
dim_stock_df = None
trades_df = None
daily_trade_prices_df = None
dim_date_df = None


def read_file(file_path):
    """
    Read CSV file from data directory.
    
    Args:
        file_path: Filename to read
        
    Returns:
        pd.DataFrame: Loaded dataframe
    """
    datasets_dir = 'data/'
    full_path = datasets_dir + file_path
    
    if full_path.endswith('.csv'):
        logger.info(f"Reading file: {full_path}")
        return pd.read_csv(full_path)
    else:
        raise ValueError("Unsupported file format")


def load_data():
    """
    Load all required datasets into global dataframes.
    """
    global dim_customer_df, dim_stock_df, trades_df, daily_trade_prices_df, dim_date_df
    
    logger.info("Loading datasets...")
    
    dim_customer_df = read_file('dim_customer.csv')
    dim_stock_df = read_file('dim_stock.csv')
    trades_df = read_file('trades.csv')
    dim_date_df = read_file('dim_date.csv')
    daily_trade_prices_df = read_file('daily_trade_prices.csv')
    
    logger.info("All datasets loaded successfully")
    logger.info(f"  - Customers: {len(dim_customer_df)} rows")
    logger.info(f"  - Stocks: {len(dim_stock_df)} rows")
    logger.info(f"  - Trades: {len(trades_df)} rows")
    logger.info(f"  - Dates: {len(dim_date_df)} rows")
    logger.info(f"  - Daily Prices: {len(daily_trade_prices_df)} rows")


def handle_outliers(df, method='cap', threshold=0.1, multiplier=1.5):
    """
    Detect and handle outliers using IQR method.
    Only handles outliers if they exceed the threshold percentage.
    
    Args:
        df: DataFrame with stock prices
        method: 'cap' or 'interpolate'
        threshold: Minimum proportion of outliers to trigger handling (default 0.1 = 10%)
        multiplier: IQR multiplier for outlier detection (default 1.5)
        
    Returns:
        pd.DataFrame: DataFrame with outliers handled
    """
    logger.info(f"Handling outliers (method={method}, threshold={threshold*100}%)")
    
    df_clean = df.copy()
    stock_cols = [col for col in df.columns if col != 'date']
    
    total_outliers_handled = 0
    columns_processed = 0
    
    for col in stock_cols:
        # Calculate IQR bounds
        q1 = df[col].quantile(0.25)
        q3 = df[col].quantile(0.75)
        iqr = q3 - q1
        lower_bound = q1 - multiplier * iqr
        upper_bound = q3 + multiplier * iqr
        
        # Detect outliers
        outlier_mask = (df[col] < lower_bound) | (df[col] > upper_bound)
        outlier_count = outlier_mask.sum()
        outlier_proportion = outlier_count / len(df)
        
        # Only handle if outliers exceed threshold
        if outlier_proportion >= threshold:
            columns_processed += 1
            total_outliers_handled += outlier_count
            
            if method == 'cap':
                # Cap at IQR bounds
                df_clean[col] = df_clean[col].clip(lower=lower_bound, upper=upper_bound)
            elif method == 'interpolate':
                # Replace outliers with NaN and interpolate
                df_clean.loc[outlier_mask, col] = np.nan
                df_clean[col] = df_clean[col].interpolate(method='linear').fillna(method='bfill').fillna(method='ffill')
            
            logger.info(f"  - {col}: {outlier_count} outliers ({outlier_proportion*100:.2f}%) handled")
    
    logger.info(f"Outlier handling complete: {total_outliers_handled} outliers in {columns_processed} columns")
    return df_clean


def handle_missing_values(df, method='ffill'):
    """
    Impute missing values in stock price data.
    
    Args:
        df: DataFrame with potential missing values
        method: 'ffill', 'bfill', or 'interpolate'
        
    Returns:
        pd.DataFrame: DataFrame with missing values handled
    """
    logger.info(f"Handling missing values (method={method})")
    
    df_filled = df.copy()
    stock_cols = [col for col in df.columns if col != 'date']
    
    # Count missing values before
    missing_before = df_filled[stock_cols].isna().sum().sum()
    
    if method == 'ffill':
        df_filled[stock_cols] = df_filled[stock_cols].fillna(method='ffill').fillna(method='bfill')
    elif method == 'bfill':
        df_filled[stock_cols] = df_filled[stock_cols].fillna(method='bfill').fillna(method='ffill')
    elif method == 'interpolate':
        df_filled[stock_cols] = df_filled[stock_cols].interpolate(method='linear').fillna(method='bfill').fillna(method='ffill')
    
    # Count missing values after
    missing_after = df_filled[stock_cols].isna().sum().sum()
    
    logger.info(f"Missing values: {missing_before} before, {missing_after} after imputation")
    
    return df_filled


def integrate_data():
    """
    Integrate all datasets into a unified dataframe with required schema.
    
    Returns:
        pd.DataFrame: Integrated dataframe
    """
    logger.info("Starting data integration...")
    
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
    
    return merged_df


def save_sample_csv(df, output_path='output/sample_output.csv', n_rows=10):
    """
    Save a sample of the dataframe to CSV.
    
    Args:
        df: DataFrame to sample
        output_path: Path to save CSV
        n_rows: Number of rows to save
    """
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Save sample
    sample_df = df.head(n_rows)
    sample_df.to_csv(output_path, index=False)
    
    logger.info(f"Saved {n_rows} sample rows to {output_path}")


def wait_for_db(max_retries=30, retry_interval=2):
    """
    Wait for database to be ready and initialize it.
    
    Args:
        max_retries: Maximum number of connection attempts
        retry_interval: Seconds to wait between retries
    """
    logger.info("Waiting for database connection...")
    
    for attempt in range(max_retries):
        try:
            initialize_database()
            logger.info("Database initialized successfully")
            return True
        except Exception as e:
            logger.warning(f"Connection attempt {attempt + 1}/{max_retries} failed: {e}")
        
        if attempt < max_retries - 1:
            time.sleep(retry_interval)
    
    raise Exception("Failed to connect to database after maximum retries")


def main():
    """
    Main execution function for the data pipeline.
    """
    try:
        logger.info("=" * 60)
        logger.info("Stock Portfolio Data Pipeline")
        logger.info("=" * 60)
        
        # Step 1: Load data
        load_data()
        
        # Step 2: Clean data
        logger.info("\n--- Data Cleaning ---")
        global cleaned_daily_trade_prices_df
        cleaned_daily_trade_prices_df = handle_outliers(
            daily_trade_prices_df, 
            method='cap', 
            threshold=0.1
        )
        cleaned_daily_trade_prices_df = handle_missing_values(
            cleaned_daily_trade_prices_df, 
            method='ffill'
        )
        
        # Step 3: Integrate data
        logger.info("\n--- Data Integration ---")
        merged_df = integrate_data()
        
        # Step 4: Display results
        logger.info("\n--- Results ---")
        logger.info("Integrated DataFrame Info:")
        merged_df.info()
        print("\nFirst 10 rows:")
        print(merged_df.head(10))
        
        # Step 5: Save sample CSV
        logger.info("\n--- Saving Sample CSV ---")
        save_sample_csv(merged_df)
        
        # Step 6: Wait for database and save
        logger.info("\n--- Database Operations ---")
        wait_for_db()
        logger.info("Saving to database...")
        save_to_db(merged_df, 'cleaned_stock_trades')
        
        logger.info("\n" + "=" * 60)
        logger.info("Pipeline completed successfully!")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        raise


if __name__ == '__main__':
    main()
