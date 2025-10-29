# Real-Time Stock Portfolio Analytics Platform

## Project Overview
An end-to-end data engineering pipeline for ingesting, cleaning, integrating, and storing trading and portfolio data in a containerized PostgreSQL warehouse.

## Dataset Description

### Output DataFrame Schema

The integrated dataset contains the following columns:

- **transaction_id**: Unique identifier for each trading transaction in the system.
- **timestamp**: Date and time when the transaction was executed.
- **customer_id**: Unique identifier for the customer who executed the transaction.
- **stock_ticker**: Stock symbol representing the traded security (e.g., AAPL, GOOGL).
- **transaction_type**: Type of transaction (e.g., BUY, SELL).
- **quantity**: Number of shares traded in the transaction.
- **average_trade_size**: Average number of shares typically traded for this stock.
- **stock_price**: Price per share at the time of transaction.
- **total_trade_amount**: Total monetary value of the transaction calculated as stock_price × quantity.
- **customer_account_type**: Type of customer account (e.g., Individual, Corporate, Institutional).
- **day_name**: Name of the weekday when the transaction occurred (e.g., Monday, Tuesday).
- **is_weekend**: Boolean flag indicating whether the transaction occurred on a weekend.
- **is_holiday**: Boolean flag indicating whether the transaction occurred on a market holiday.
- **stock_liquidity_tier**: Classification of stock liquidity (e.g., High, Medium, Low).
- **stock_sector**: Economic sector to which the stock belongs (e.g., Technology, Healthcare).
- **stock_industry**: Specific industry classification within the sector (e.g., Software, Pharmaceuticals).
