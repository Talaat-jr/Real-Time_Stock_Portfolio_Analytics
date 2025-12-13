# Real-Time Stock Portfolio Analytics Platform

## Project Overview
An end-to-end data engineering pipeline for ingesting, cleaning, integrating, and storing trading and portfolio data in a containerized PostgreSQL warehouse. The platform includes real-time streaming with Kafka, distributed analytics with Spark, interactive visualizations, and an AI-powered natural language to SQL query interface.

## Quick Start

### Prerequisites
- Docker and Docker Compose installed
- At least 8GB of available RAM
- Ports 8082, 8501, 8502, 5432, 9092 available

### 1. How to Run the Pipeline

#### Initial Setup

1. **Navigate to the app directory:**
   ```bash
   cd app
   ```

2. **Create environment file (if not exists):**
   ```bash
   cp .env.example .env
   ```
   Edit `.env` and set required variables:
   - `GEMINI_API_KEY`: Your Google Gemini API key for AI agent
   - `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`: Database credentials

3. **Start all services:**
   ```bash
   docker-compose up -d
   ```

4. **Wait for services to be healthy:**
   ```bash
   # Check service status
   docker-compose ps
   
   # Wait for Airflow to initialize (may take 2-3 minutes)
   docker-compose logs -f airflow-init
   ```

5. **Start Spark Master and Worker manually (if needed):**
   ```bash
   # Start Spark Master
   docker exec spark-master /usr/local/spark/sbin/start-master.sh
   
   # Start Spark Worker
   docker exec spark-worker /usr/local/spark/sbin/start-worker.sh spark://spark-master:7077
   ```

#### Running the Pipeline

The pipeline runs automatically via Airflow DAG. To trigger it:

1. **Access Airflow UI** (see section below)
2. **Unpause the DAG:**
   - Find `stock_portfolio_pipeline_AA` in the DAG list
   - Toggle the pause/unpause button to enable it
3. **Trigger the DAG:**
   - Click on the DAG name
   - Click "Play" button to trigger a new run
   - Or wait for scheduled execution (runs daily at midnight)

#### Pipeline Stages

The pipeline executes in the following order:

1. **Data Cleaning & Integration**: Handles missing values, outliers, and integrates datasets
2. **Encoding & Stream Preparation**: Encodes categorical data and prepares streaming subset
3. **Kafka Streaming**: Produces and consumes real-time data streams
4. **Spark Analytics**: Runs distributed analytics and SQL queries
5. **Data Visualization**: Prepares data and starts visualization dashboard
6. **AI Agent Query Pipeline**: Processes natural language queries and starts AI query interface

#### Monitoring Pipeline Progress

- **Via Airflow UI**: See real-time task status and logs
- **Via Docker logs**: 
   ```bash
   docker-compose logs -f airflow-worker
   ```

### 2. How to Access the Airflow UI

#### Access URL
```
http://localhost:8082
```

#### Default Credentials
- **Username**: `airflow`
- **Password**: `airflow`

#### Features

1. **DAG Management:**
   - View all available DAGs
   - Pause/unpause DAGs
   - Trigger manual DAG runs
   - View DAG execution history

2. **Task Monitoring:**
   - Real-time task status (success, failed, running, queued)
   - Task execution logs
   - Task duration and retry information
   - XCom data exchange between tasks

3. **Pipeline Visualization:**
   - Graph view showing task dependencies
   - Gantt chart for execution timeline
   - Tree view for execution history

#### Troubleshooting Airflow Access

If you cannot access the Airflow UI:

1. **Check if the service is running:**
   ```bash
   docker ps | grep airflow-webserver
   ```

2. **Check logs:**
   ```bash
   docker-compose logs airflow-webserver
   ```

3. **Verify port mapping:**
   ```bash
   docker port stock_airflow_webserver
   ```
   Should show: `8080/tcp -> 0.0.0.0:8082`

4. **Restart the webserver:**
   ```bash
   docker-compose restart airflow-webserver
   ```

### 3. How to View the Dashboard

#### Access URL
```
http://localhost:8501
```

#### When is it Available?

The dashboard is automatically started by the Airflow pipeline after the `data_visualization.start_visualization_service` task completes. This happens after:
- Data cleaning and integration
- Encoding and stream preparation
- Kafka streaming
- Spark analytics
- Visualization data preparation

#### Dashboard Features

- **📊 Volume by Ticker**: Trading volume analysis by stock
- **💹 Price Trends**: Stock price trends by sector over time
- **🔄 Buy vs Sell**: Transaction type comparison
- **📅 Weekly Activity**: Trading patterns by day of week
- **👥 Customer Distribution**: Customer transaction analysis

For detailed dashboard documentation, see [VISUALIZATION.md](app/VISUALIZATION.md)

### 4. How to Test the AI Agent

#### Access URL
```
http://localhost:8502
```

#### When is it Available?

The AI Query Interface is automatically started by the Airflow pipeline after the `ai_agent_query_pipeline.launch_query_interface` task completes. This happens after the AI agent processes the initial query.

#### Testing the AI Agent

1. **Access the Query Interface:**
   - Open `http://localhost:8502` in your browser
   - You'll see the NL2SQL Query Interface

2. **Submit a Natural Language Query:**
   - Enter a question in natural language, for example:
     - "What was the total trading volume for technology stocks?"
     - "Show me the top 10 customers by transaction count"
     - "What is the average stock price by sector?"
     - "How many buy transactions occurred on weekends?"

3. **View Results:**
   - The AI agent will convert your query to SQL
   - Execute the query against the database
   - Show the generated SQL query

4. **View Agent Metrics:**
   - Check the sidebar for agent statistics
   - Total queries, success rate, average execution time
   - View query history and logs

#### Testing via File

You can also test by editing the query file:

1. **Edit the query file:**
   ```bash
   # On Windows (PowerShell)
   notepad app\agents\user_query.txt
   
   # On Linux/Mac
   nano app/agents/user_query.txt
   ```

2. **Add your query:**
   ```
   What was the total trading volume for technology stocks?
   ```

3. **Trigger the AI agent task in Airflow:**
   - Go to Airflow UI
   - Find `ai_agent_query_pipeline.process_with_ai_agent` task
   - Trigger it manually or wait for pipeline execution

4. **View results:**
   - Check `app/agents/AGENT_LOGS.JSON` for query logs
   - Check `app/agents/logs/` for detailed logs
   - Check `app/agents/generated_results/` for CSV exports

#### Troubleshooting AI Agent

If the AI agent is not working:

1. **Check if GEMINI_API_KEY is set:**
   ```bash
   docker exec stock_airflow_worker env | grep GEMINI_API_KEY
   ```

2. **Check agent logs:**
   ```bash
   docker exec stock_airflow_worker cat /opt/airflow/logs/ai_query_interface.log
   ```

3. **Verify the query interface is running:**
   ```bash
   docker exec stock_airflow_worker pgrep -f 'streamlit run.*ai_query_interface.py'
   ```

4. **Check agent task logs in Airflow:**
   - Go to Airflow UI
   - Find `ai_agent_query_pipeline.process_with_ai_agent` task
   - View task logs for errors

## Dataset Description

### Output DataFrame Schema

The integrated dataset contains the following columns:

- **transaction_id**: Unique identifier for each trading transaction in the system.
- **timestamp**: Date and time when the transaction was executed.
- **customer_id**: Unique identifier for the customer who executed the transaction.
- **stock_ticker**: Stock symbol representing the traded security (e.g., AAPL, GOOGL).
- **transaction_type**: Type of transaction (e.g., BUY, SELL).
- **quantity**: Number of shares traded in the transaction.
- **average_trade_size**: Average number of shares typically traded by this customer
- **stock_price**: Price per share at the time of transaction.
- **total_trade_amount**: Total monetary value of the transaction calculated as stock_price × quantity.
- **customer_account_type**: Type of customer account (e.g., Individual, Corporate, Institutional).
- **day_name**: Name of the weekday when the transaction occurred (e.g., Monday, Tuesday).
- **is_weekend**: Boolean flag indicating whether the transaction occurred on a weekend.
- **is_holiday**: Boolean flag indicating whether the transaction occurred on a market holiday.
- **stock_liquidity_tier**: Classification of stock liquidity (e.g., High, Medium, Low).
- **stock_sector**: Economic sector to which the stock belongs (e.g., Technology, Healthcare).
- **stock_industry**: Specific industry classification within the sector (e.g., Software, Pharmaceuticals).


## Architecture

For a detailed visual representation of the system architecture, see [ARCHITECTURE.md](ARCHITECTURE.md).

The architecture diagram shows:
- Data sources and ingestion
- Pipeline stages and data flow
- Service components and their interactions
- Storage layers and outputs
- User-facing interfaces

## Additional Resources

- **Architecture Diagram**: See [ARCHITECTURE.md](ARCHITECTURE.md) for the complete system architecture
- **Dashboard Documentation**: See [app/VISUALIZATION.md](app/VISUALIZATION.md) for detailed dashboard features
- **Pipeline Architecture**: Check DAG definition in `app/src/dags/stock_portfolio_pipeline.py`
- **Task Functions**: See `app/src/dags/dag_tasks.py` for implementation details
