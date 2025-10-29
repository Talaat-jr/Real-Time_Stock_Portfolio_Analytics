AI PROMPTS AND OUTPUTS FOR METADATA GENERATION
=================================================

PROMPT: Dataset Description Generation
-----------------------------------------
Prompt:
"I have a stock portfolio analytics dataset with the following columns:
- transaction_id
- timestamp
- customer_id
- stock_ticker
- transaction_type
- quantity
- average_trade_size
- stock_price
- total_trade_amount (calculated as stock_price * quantity)
- customer_account_type
- day_name
- is_weekend
- is_holiday
- stock_liquidity_tier
- stock_sector
- stock_industry

Please provide a one-sentence description for each column that clearly explains its purpose and content in the context of stock trading and portfolio management."

Output:
--------
transaction_id: Unique identifier for each trading transaction in the system.

timestamp: Date and time when the transaction was executed.

customer_id: Unique identifier for the customer who executed the transaction.

stock_ticker: Stock symbol representing the traded security (e.g., AAPL, GOOGL).

transaction_type: Type of transaction (e.g., BUY, SELL).

quantity: Number of shares traded in the transaction.

average_trade_size: Average number of shares typically traded for this stock.

stock_price: Price per share at the time of transaction.

total_trade_amount: Total monetary value of the transaction calculated as stock_price × quantity.

customer_account_type: Type of customer account (e.g., Individual, Corporate, Institutional).

day_name: Name of the weekday when the transaction occurred (e.g., Monday, Tuesday).

is_weekend: Boolean flag indicating whether the transaction occurred on a weekend.

is_holiday: Boolean flag indicating whether the transaction occurred on a market holiday.

stock_liquidity_tier: Classification of stock liquidity (e.g., High, Medium, Low).

stock_sector: Economic sector to which the stock belongs (e.g., Technology, Healthcare).

stock_industry: Specific industry classification within the sector (e.g., Software, Pharmaceuticals).


TOOL USED: Claude (Anthropic)
DATE: October 30, 2025
VERSION: Claude Sonnet 4.5
