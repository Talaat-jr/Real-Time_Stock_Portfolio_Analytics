"""
Static Schema Configuration for NL2SQL Agent
Provides fallback schema and statistics when database queries fail.
"""

# Static schema definition for stock_trades_integrated table
STATIC_SCHEMA = {
    "table_name": "stock_trades_integrated",
    "columns": [
        {
            "name": "transaction_id",
            "type": "bigint",
            "nullable": False,
            "description": "Unique identifier for each trading transaction"
        },
        {
            "name": "timestamp",
            "type": "timestamp",
            "nullable": False,
            "description": "Date and time when the transaction was executed"
        },
        {
            "name": "customer_id",
            "type": "bigint",
            "nullable": False,
            "description": "Unique identifier for the customer"
        },
        {
            "name": "stock_ticker",
            "type": "varchar",
            "nullable": False,
            "description": "Stock symbol (e.g., AAPL, GOOGL, MSFT)"
        },
        {
            "name": "transaction_type",
            "type": "varchar",
            "nullable": False,
            "description": "Type of transaction: BUY or SELL"
        },
        {
            "name": "quantity",
            "type": "integer",
            "nullable": False,
            "description": "Number of shares traded"
        },
        {
            "name": "average_trade_size",
            "type": "float",
            "nullable": True,
            "description": "Average number of shares typically traded"
        },
        {
            "name": "stock_price",
            "type": "float",
            "nullable": False,
            "description": "Price per share at transaction time"
        },
        {
            "name": "total_trade_amount",
            "type": "float",
            "nullable": False,
            "description": "Total monetary value (stock_price × quantity)"
        },
        {
            "name": "customer_account_type",
            "type": "varchar",
            "nullable": True,
            "description": "Account type: Individual, Corporate, or Institutional"
        },
        {
            "name": "day_name",
            "type": "varchar",
            "nullable": True,
            "description": "Day of week: Monday through Sunday"
        },
        {
            "name": "is_weekend",
            "type": "boolean",
            "nullable": True,
            "description": "True if transaction occurred on weekend"
        },
        {
            "name": "is_holiday",
            "type": "boolean",
            "nullable": True,
            "description": "True if transaction occurred on market holiday"
        },
        {
            "name": "stock_liquidity_tier",
            "type": "varchar",
            "nullable": True,
            "description": "Liquidity classification: High, Medium, or Low"
        },
        {
            "name": "stock_sector",
            "type": "varchar",
            "nullable": True,
            "description": "Economic sector (e.g., Technology, Healthcare, Finance)"
        },
        {
            "name": "stock_industry",
            "type": "varchar",
            "nullable": True,
            "description": "Specific industry within sector"
        },
        {
            "name": "date",
            "type": "date",
            "nullable": True,
            "description": "Transaction date (extracted from timestamp)"
        },
        {
            "name": "year",
            "type": "integer",
            "nullable": True,
            "description": "Year of transaction"
        },
        {
            "name": "month",
            "type": "integer",
            "nullable": True,
            "description": "Month of transaction (1-12)"
        },
        {
            "name": "month_name",
            "type": "varchar",
            "nullable": True,
            "description": "Month name (January through December)"
        }
    ]
}

# Static statistics for common queries
STATIC_STATISTICS = {
    "transaction_type": {
        "unique_values": 2,
        "common_values": ["BUY", "SELL"],
        "null_count": 0
    },
    "customer_account_type": {
        "unique_values": 3,
        "common_values": ["Individual", "Corporate", "Institutional"],
        "null_count": 0
    },
    "stock_liquidity_tier": {
        "unique_values": 3,
        "common_values": ["High", "Medium", "Low"],
        "null_count": 0
    },
    "stock_sector": {
        "unique_values": 11,
        "common_values": [
            "Technology", "Healthcare", "Finance", 
            "Energy", "Consumer Goods"
        ],
        "null_count": 0
    },
    "day_name": {
        "unique_values": 7,
        "common_values": [
            "Monday", "Tuesday", "Wednesday", 
            "Thursday", "Friday", "Saturday", "Sunday"
        ],
        "null_count": 0
    },
    "is_weekend": {
        "unique_values": 2,
        "common_values": [True, False],
        "null_count": 0
    },
    "is_holiday": {
        "unique_values": 2,
        "common_values": [True, False],
        "null_count": 0
    },
    "month_name": {
        "unique_values": 12,
        "common_values": [
            "January", "February", "March", "April",
            "May", "June", "July", "August",
            "September", "October", "November", "December"
        ],
        "null_count": 0
    }
}

def get_static_schema_context() -> dict:
    """
    Get static schema context for fallback scenarios.
    
    Returns:
        dict: Static schema and statistics
    """
    return {
        "schema": STATIC_SCHEMA,
        "statistics": STATIC_STATISTICS,
        "source": "static_fallback",
        "row_count": "~10000",
        "last_updated": "static"
    }
