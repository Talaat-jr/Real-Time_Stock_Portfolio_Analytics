#!/bin/bash
set -e

# Fix permissions on mounted volumes
if [ -d "/opt/airflow/logs" ]; then
    chmod -R 775 /opt/airflow/logs 2>/dev/null || true
fi
if [ -d "/opt/airflow/dags" ]; then
    chmod -R 775 /opt/airflow/dags 2>/dev/null || true
fi
if [ -d "/opt/airflow/plugins" ]; then
    chmod -R 775 /opt/airflow/plugins 2>/dev/null || true
fi
if [ -d "/opt/airflow/data" ]; then
    chmod -R 775 /opt/airflow/data 2>/dev/null || true
fi
if [ -d "/opt/airflow/config" ]; then
    chmod -R 775 /opt/airflow/config 2>/dev/null || true
fi
if [ -d "/opt/airflow/output" ]; then
    chmod -R 775 /opt/airflow/output 2>/dev/null || true
fi

# Execute the original entrypoint
exec /entrypoint "$@"
