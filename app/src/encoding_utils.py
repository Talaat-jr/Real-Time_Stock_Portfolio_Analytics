import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
import os
import logging

logger = logging.getLogger(__name__)


def encode_data(df):
    """
    Encode categorical columns in the dataframe.
    
    Encoding strategy:
    - Label Encoding: For ordinal or binary categorical variables
      (stock_ticker, transaction_type, customer_account_type, day_name)
    - One-Hot Encoding: For nominal categorical with few unique values
      (is_weekend, is_holiday already binary 0/1)
    - Keep as-is: stock_sector, stock_industry (high cardinality, can use label encoding)
    
    Args:
        df: DataFrame to encode
        
    Returns:
        tuple: (encoded_df, lookup_tables_dict)
    """
    logger.info("Starting data encoding...")
    
    df_encoded = df.copy()
    lookup_tables = {}
    
    # Columns to encode with Label Encoding
    label_encode_cols = [
        'stock_ticker',
        'transaction_type', 
        'customer_account_type',
        'day_name',
        'stock_sector',
        'stock_industry'
    ]
    
    # Binary columns (already 0/1 or True/False)
    binary_cols = ['is_weekend', 'is_holiday']
    
    # Apply Label Encoding
    for col in label_encode_cols:
        if col in df_encoded.columns:
            logger.info(f"Label encoding: {col}")
            
            # Create label encoder
            le = LabelEncoder()
            
            # Handle missing values
            mask_notna = df_encoded[col].notna()
            
            if mask_notna.sum() > 0:
                # Fit and transform non-null values
                df_encoded.loc[mask_notna, col] = le.fit_transform(
                    df_encoded.loc[mask_notna, col]
                )
                
                # Create lookup table
                lookup_df = pd.DataFrame({
                    'Column_Name': col,
                    'Original_Value': le.classes_,
                    'Encoded_Value': range(len(le.classes_))
                })
                lookup_tables[col] = lookup_df
                
                logger.info(f"  - {col}: {len(le.classes_)} unique values encoded")
    
    # Handle binary columns (ensure they're numeric)
    for col in binary_cols:
        if col in df_encoded.columns:
            logger.info(f"Processing binary column: {col}")
            
            # Convert boolean to int if needed
            if df_encoded[col].dtype == 'bool':
                df_encoded[col] = df_encoded[col].astype(int)
            else:
                df_encoded[col] = df_encoded[col]
            
            # Create lookup table for binary
            unique_vals = df_encoded[col].dropna().unique()
            lookup_df = pd.DataFrame({
                'Column_Name': col,
                'Original_Value': unique_vals,
                'Encoded_Value': unique_vals
            })
            lookup_tables[col] = lookup_df
    
    logger.info(f"Encoding complete. {len(lookup_tables)} lookup tables created.")
    
    return df_encoded, lookup_tables


def encode_single_record(record, lookup_tables):
    """
    Encode a single record using existing lookup tables.
    Used for streaming data processing.
    
    Args:
        record: Dictionary or Series representing one row
        lookup_tables: Dictionary of lookup tables from encode_data()
        
    Returns:
        dict: Encoded record
    """
    if isinstance(record, pd.Series):
        record = record.to_dict()
    
    encoded_record = record.copy()
    
    # Columns to encode
    encode_cols = [
        'stock_ticker',
        'transaction_type',
        'customer_account_type', 
        'day_name',
        'stock_sector',
        'stock_industry',
        'is_weekend',
        'is_holiday'
    ]
    
    for col in encode_cols:
        if col in record and col in lookup_tables:
            original_value = record[col]
            
            # Find encoded value in lookup table
            lookup_df = lookup_tables[col]
            match = lookup_df[lookup_df['Original_Value'] == original_value]
            
            if not match.empty:
                encoded_record[col] = int(match.iloc[0]['Encoded_Value'])
            else:
                # Handle unseen category (use -1 or most frequent)
                logger.warning(f"Unknown value '{original_value}' for column '{col}'. Using -1.")
                encoded_record[col] = -1
    
    return encoded_record


def save_lookup_tables(lookup_tables, output_dir='output/lookup_tables'):
    """
    Save all lookup tables to CSV files.
    
    Args:
        lookup_tables: Dictionary of lookup DataFrames
        output_dir: Directory to save lookup tables
    """
    os.makedirs(output_dir, exist_ok=True)
    
    logger.info(f"Saving {len(lookup_tables)} lookup tables to {output_dir}...")
    
    for col_name, lookup_df in lookup_tables.items():
        output_path = os.path.join(output_dir, f'lookup_{col_name}.csv')
        lookup_df.to_csv(output_path, index=False)
        logger.info(f"  - Saved: lookup_{col_name}.csv ({len(lookup_df)} mappings)")
    
    # Create a combined lookup table
    combined_lookup = pd.concat(lookup_tables.values(), ignore_index=True)
    combined_path = os.path.join(output_dir, 'lookup_all_encodings.csv')
    combined_lookup.to_csv(combined_path, index=False)
    logger.info(f"  - Saved: lookup_all_encodings.csv (combined)")


def load_lookup_tables(input_dir='output/lookup_tables'):
    """
    Load lookup tables from CSV files.
    
    Args:
        input_dir: Directory containing lookup table CSVs
        
    Returns:
        dict: Dictionary of lookup DataFrames
    """
    lookup_tables = {}
    
    if not os.path.exists(input_dir):
        logger.warning(f"Lookup tables directory not found: {input_dir}")
        return lookup_tables
    
    # Load individual lookup files
    for filename in os.listdir(input_dir):
        if filename.startswith('lookup_') and filename.endswith('.csv') and filename != 'lookup_all_encodings.csv':
            col_name = filename.replace('lookup_', '').replace('.csv', '')
            file_path = os.path.join(input_dir, filename)
            lookup_tables[col_name] = pd.read_csv(file_path)
            logger.info(f"Loaded lookup table: {col_name}")
    
    return lookup_tables


def create_imputation_lookup_table(df, column, imputed_value, output_dir='output/lookup_tables'):
    """
    Create a lookup table for constant/arbitrary imputation.
    
    Args:
        df: DataFrame
        column: Column name that was imputed
        imputed_value: The constant value used for imputation
        output_dir: Directory to save lookup table
        
    Returns:
        pd.DataFrame: Lookup table
    """
    # Find rows where imputation was applied (assuming NaN was replaced)
    # This is for documentation purposes
    
    lookup_df = pd.DataFrame({
        'Column_Name': [column],
        'Original_Value': ['NaN'],
        'Imputed_Value': [imputed_value]
    })
    
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f'imputation_{column}.csv')
    lookup_df.to_csv(output_path, index=False)
    
    logger.info(f"Saved imputation lookup table: imputation_{column}.csv")
    
    return lookup_df
