import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os
from typing import Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_db_config() -> dict:
    """
    Get database configuration from environment variables.
    
    Returns:
        dict: Database configuration parameters
    """
    return {
        'host': os.getenv('POSTGRES_HOST', 'localhost'),
        'port': os.getenv('POSTGRES_PORT', '5432'),
        'database': os.getenv('POSTGRES_DB', 'stock_portfolio'),
        'user': os.getenv('POSTGRES_USER', 'postgres'),
        'password': os.getenv('POSTGRES_PASSWORD', 'postgres')
    }


def get_db_connection_string(database: Optional[str] = None) -> str:
    """
    Construct database connection string from environment variables.
    
    Args:
        database: Optional database name override
    
    Returns:
        str: SQLAlchemy connection string
    """
    config = get_db_config()
    db_name = database or config['database']
    
    return f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{db_name}"


def create_database_if_not_exists() -> bool:
    """
    Create the database if it doesn't exist using psycopg2.
    This is useful for initial setup.
    
    Returns:
        bool: True if database was created, False if it already existed
    """
    config = get_db_config()
    target_db = config['database']
    
    try:
        # Connect to PostgreSQL server (default 'postgres' database)
        conn = psycopg2.connect(
            host=config['host'],
            database='postgres',  # Connect to default database first
            user=config['user'],
            password=config['password'],
            port=config['port']
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Check if database exists
        cursor.execute(
            "SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s",
            (target_db,)
        )
        exists = cursor.fetchone()
        
        if not exists:
            # Create database
            cursor.execute(f'CREATE DATABASE {target_db}')
            logger.info(f"Database '{target_db}' created successfully")
            created = True
        else:
            logger.info(f"Database '{target_db}' already exists")
            created = False
        
        cursor.close()
        conn.close()
        return created
        
    except psycopg2.Error as e:
        logger.error(f"Error creating database: {e}")
        raise


def get_psycopg2_connection():
    """
    Create a direct psycopg2 connection for administrative tasks.
    
    Returns:
        psycopg2.connection: Database connection
    """
    config = get_db_config()
    
    try:
        conn = psycopg2.connect(
            host=config['host'],
            database=config['database'],
            user=config['user'],
            password=config['password'],
            port=config['port']
        )
        logger.info("Direct psycopg2 connection established")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Error creating psycopg2 connection: {e}")
        raise


def create_db_engine():
    """
    Create and return a SQLAlchemy engine for DataFrame operations.
    
    Returns:
        sqlalchemy.engine.Engine: Database engine
    """
    try:
        connection_string = get_db_connection_string()
        engine = create_engine(connection_string, pool_pre_ping=True)
        logger.info("SQLAlchemy engine created successfully")
        return engine
    except Exception as e:
        logger.error(f"Error creating database engine: {e}")
        raise


def test_connection() -> bool:
    """
    Test database connection using both methods.
    
    Returns:
        bool: True if connection successful, False otherwise
    """
    try:
        # Test with SQLAlchemy
        engine = create_db_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        engine.dispose()
        
        # Test with psycopg2
        conn = get_psycopg2_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        db_version = cursor.fetchone()
        logger.info(f"PostgreSQL version: {db_version[0]}")
        cursor.close()
        conn.close()
        
        logger.info("Database connection test successful")
        return True
    except Exception as e:
        logger.error(f"Database connection test failed: {e}")
        return False


def initialize_database():
    """
    Initialize database: create if not exists and test connection.
    This should be called before running the pipeline.
    """
    logger.info("Initializing database...")
    create_database_if_not_exists()
    test_connection()
    logger.info("Database initialization complete")


def table_exists(table_name: str) -> bool:
    """
    Check if a table exists in the database.
    
    Args:
        table_name: Name of the table to check
    
    Returns:
        bool: True if table exists, False otherwise
    """
    try:
        conn = get_psycopg2_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            );
        """, (table_name,))
        
        exists = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        
        return exists
    except psycopg2.Error as e:
        logger.error(f"Error checking table existence: {e}")
        raise


def save_to_db(df: pd.DataFrame, table_name: str, if_exists: str = 'replace') -> None:
    """
    Save DataFrame to PostgreSQL database using SQLAlchemy.
    
    Args:
        df: pandas DataFrame to save
        table_name: Name of the target table
        if_exists: How to behave if table exists {'fail', 'replace', 'append'}
    
    Raises:
        Exception: If save operation fails
    """
    try:
        engine = create_db_engine()
        
        # Check if table exists before operation
        table_existed = table_exists(table_name)
        
        # Save DataFrame to database
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists=if_exists,
            index=False,
            method='multi',  # Faster bulk inserts
            chunksize=1000   # Insert in chunks for better performance
        )
        
        action = "Updated" if table_existed and if_exists == 'replace' else "Created"
        logger.info(f"{action} table '{table_name}' with {len(df)} rows")
        
        # Verify using psycopg2
        conn = get_psycopg2_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        logger.info(f"Verification: Table '{table_name}' contains {count} rows")
        cursor.close()
        conn.close()
            
    except Exception as e:
        logger.error(f"Error saving data to database: {e}")
        raise
    finally:
        engine.dispose()


def read_from_db(table_name: str, query: Optional[str] = None) -> pd.DataFrame:
    """
    Read data from PostgreSQL database using SQLAlchemy.
    
    Args:
        table_name: Name of the table to read
        query: Optional SQL query (if None, reads entire table)
    
    Returns:
        pd.DataFrame: Data from database
    """
    try:
        engine = create_db_engine()
        
        if query is None:
            query = f"SELECT * FROM {table_name}"
        
        df = pd.read_sql(query, engine)
        logger.info(f"Successfully read {len(df)} rows from database")
        
        return df
        
    except Exception as e:
        logger.error(f"Error reading data from database: {e}")
        raise
    finally:
        engine.dispose()


def execute_sql(sql: str, params: Optional[tuple] = None) -> list:
    """
    Execute raw SQL query using psycopg2.
    Useful for administrative tasks.
    
    Args:
        sql: SQL query to execute
        params: Optional query parameters
    
    Returns:
        list: Query results
    """
    try:
        conn = get_psycopg2_connection()
        cursor = conn.cursor()
        
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        
        # Fetch results if it's a SELECT query
        if cursor.description:
            results = cursor.fetchall()
        else:
            results = []
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info("SQL query executed successfully")
        return results
        
    except psycopg2.Error as e:
        logger.error(f"Error executing SQL: {e}")
        raise


def get_table_info(table_name: str) -> dict:
    """
    Get comprehensive information about a table.
    
    Args:
        table_name: Name of the table
    
    Returns:
        dict: Table metadata (row count, column names, types, etc.)
    """
    try:
        conn = get_psycopg2_connection()
        cursor = conn.cursor()
        
        # Get row count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        
        # Get column info
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length,
                   is_nullable, column_default
            FROM information_schema.columns 
            WHERE table_name = %s
            ORDER BY ordinal_position
        """, (table_name,))
        columns = cursor.fetchall()
        
        # Get table size
        cursor.execute(f"""
            SELECT pg_size_pretty(pg_total_relation_size('{table_name}'))
        """)
        table_size = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        info = {
            'table_name': table_name,
            'row_count': row_count,
            'table_size': table_size,
            'columns': [
                {
                    'name': col[0],
                    'type': col[1],
                    'max_length': col[2],
                    'nullable': col[3],
                    'default': col[4]
                } for col in columns
            ]
        }
        
        logger.info(f"Retrieved info for table '{table_name}'")
        return info
        
    except psycopg2.Error as e:
        logger.error(f"Error getting table info: {e}")
        raise


def drop_table(table_name: str, cascade: bool = False) -> None:
    """
    Drop a table from the database.
    
    Args:
        table_name: Name of the table to drop
        cascade: If True, automatically drop objects that depend on the table
    """
    try:
        conn = get_psycopg2_connection()
        cursor = conn.cursor()
        
        cascade_clause = "CASCADE" if cascade else ""
        cursor.execute(f"DROP TABLE IF EXISTS {table_name} {cascade_clause}")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Table '{table_name}' dropped successfully")
        
    except psycopg2.Error as e:
        logger.error(f"Error dropping table: {e}")
        raise
