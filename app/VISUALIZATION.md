# Real-Time Stock Portfolio Analytics Dashboard

## Overview

The Real-Time Stock Portfolio Analytics Dashboard is an interactive Streamlit application that provides comprehensive visualization and analysis of stock trading data. The dashboard is automatically started by the Airflow pipeline after data processing and visualization preparation stages are complete.

## Accessing the Dashboard

### URL
The dashboard is accessible at:
```
http://localhost:8501
```

### When is it Available?

The dashboard is automatically started by the Airflow DAG task `data_visualization.start_visualization_service`. This task runs after:
1. Data cleaning and integration
2. Encoding and stream preparation
3. Kafka streaming
4. Spark analytics
5. Visualization data preparation


## Dashboard Features

### 1. **Core Analytics Tabs**

The dashboard is organized into five main analytical sections:

#### 📊 Volume by Ticker
- Total trading volume for each stock ticker
- Interactive bar chart with color-coded volumes
- Top 5 and Bottom 5 performers table

#### 💹 Price Trends
- Stock price trends by sector over time
- Line charts showing price movements
- Sector-wise analysis

#### 🔄 Buy vs Sell
- Comparison of buy and sell transactions
- Transaction type distribution
- Volume and amount analysis

#### 📅 Weekly Activity
- Trading activity by day of the week
- Weekend vs weekday comparison
- Holiday vs non-holiday trading patterns

#### 👥 Customer Distribution
- Customer transaction distribution
- Top customers by trading volume
- Account type analysis

### 2. **Interactive Filters**

The sidebar provides powerful filtering options:

- **Date Range Filter**: Select specific date ranges for analysis
- **Stock Ticker Filter**: Filter by specific stock tickers
- **Sector Filter**: Analyze by stock sectors
- **Industry Filter**: Filter by stock industries
- **Account Type Filter**: Filter by customer account types
- **Transaction Type Filter**: Filter by BUY/SELL transactions

### 3. **Key Performance Indicators (KPIs)**

The dashboard displays real-time metrics:
- Total transactions
- Total trading volume
- Total trade amount
- Unique stocks
- Unique customers
- Average trade size

### 4. **Advanced Visualizations**

- **Sunburst Chart**: Hierarchical view of sectors and industries
- **Heatmaps**: Trading patterns visualization
- **Time Series Charts**: Price and volume trends over time
- **Distribution Charts**: Customer and stock distributions

### 5. **Data Table View**

- Expandable data table showing all filtered transactions
- Export capabilities
- Real-time data updates

## Data Sources

The dashboard loads data in the following priority:

1. **Post-Streaming CSV** (`/opt/airflow/output/FULL_STOCKS_DECODED.csv`)
   - Most up-to-date data including Kafka streamed records
   - Preferred source for real-time analysis

2. **Database** (`stock_trades_integrated` table)
   - Fallback if CSV is not available
   - Pre-streaming integrated data

3. **Aggregated Data Files**
   - Pre-computed aggregations for faster loading
   - Located in `/opt/airflow/output/` directory