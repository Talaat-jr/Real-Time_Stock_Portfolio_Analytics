from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import sys
import os

# Add src directory to path
sys.path.append('/opt/airflow/src')

# Import custom modules
from dag_tasks import (
    # Stage 1: Data Cleaning & Integration
    clean_missing_values_task,
    detect_outliers_task,
    integrate_datasets_task,
    load_to_postgres_task,
    
    # Stage 2: Encoding & Stream Preparation
    prepare_streaming_data_task,
    encode_categorical_data_task,
    
    # Stage 3: Kafka Streaming
    consume_and_process_stream_task,
    save_final_to_postgres_task,
    
    # Stage 4: Spark Analytics
    initialize_spark_session_task,
    run_spark_analytics_task,
    
    # Stage 5: Data Visualization
    prepare_visualization_task,
    start_streamlit_dashboard_task,
    
    # Stage 6: AI Agent
    process_with_ai_agent_task
)

# Default arguments
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 9),  # Yesterday
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'stock_portfolio_pipeline_team_name',
    default_args=default_args,
    description='End-to-end stock portfolio analytics pipeline',
    schedule_interval='@daily',
    catchup=False,
    tags=['data-engineering', 'stocks', 'analytics'],
) as dag:
    with TaskGroup('data_cleaning_integration') as data_cleaning_integration:
        clean_missing_values = PythonOperator(
            task_id='clean_missing_values',
            python_callable=clean_missing_values_task,
        )

        detect_outliers = PythonOperator(
            task_id='detect_outliers',
            python_callable=detect_outliers_task,
        )

        integrate_datasets = PythonOperator(
            task_id='integrate_datasets',
            python_callable=integrate_datasets_task,
        )

        load_to_postgres = PythonOperator(
            task_id='load_to_postgres',
            python_callable=load_to_postgres_task,
        )

        clean_missing_values >> detect_outliers >> integrate_datasets >> load_to_postgres

    with TaskGroup('encoding_stream_preparation') as encoding_stream_preparation:
        prepare_streaming_data = PythonOperator(
            task_id='prepare_streaming_data',
            python_callable=prepare_streaming_data_task,
        )

        encode_categorical_data = PythonOperator(
            task_id='encode_categorical_data',
            python_callable=encode_categorical_data_task,
        )

        prepare_streaming_data >> encode_categorical_data

    with TaskGroup('kafka_streaming') as kafka_streaming:
        start_kafka_producer = BashOperator(
            task_id='start_kafka_producer',
            bash_command='cd /opt/airflow && python src/producer.py',
        )

        consume_and_process_stream = PythonOperator(
            task_id='consume_and_process_stream',
            python_callable=consume_and_process_stream_task,
        )

        save_final_to_postgres = PythonOperator(
            task_id='save_final_to_postgres',
            python_callable=save_final_to_postgres_task,
        )

        start_kafka_producer >> consume_and_process_stream >> save_final_to_postgres

    with TaskGroup('spark_analytics') as spark_analytics:
        initialize_spark_session = PythonOperator(
            task_id='initialize_spark_session',
            python_callable=initialize_spark_session_task,
        )

        run_spark_analytics = PythonOperator(
            task_id='run_spark_analytics',
            python_callable=run_spark_analytics_task,
        )

        initialize_spark_session >> run_spark_analytics

    with TaskGroup('data_visualization') as data_visualization:
        prepare_visualization = PythonOperator(
            task_id='prepare_visualization',
            python_callable=prepare_visualization_task,
        )

        # Start Streamlit dashboard after data is prepared
        # Dashboard will be accessible at http://localhost:8501
        start_visualization_service = PythonOperator(
            task_id='start_visualization_service',
            python_callable=start_streamlit_dashboard_task,
        )

        prepare_visualization >> start_visualization_service

    with TaskGroup('ai_agent_query_pipeline') as ai_agent_query_pipeline:
        process_with_ai_agent = PythonOperator(
            task_id='process_with_ai_agent',
            python_callable=process_with_ai_agent_task,
        )


# Stages Dependencies

data_cleaning_integration >> encoding_stream_preparation >> kafka_streaming >> spark_analytics
spark_analytics >> data_visualization
spark_analytics >> ai_agent_query_pipeline
