#!/usr/bin/env bash
# Usage: ./count_tasks.sh [dag_id]
set -euo pipefail

DAG_ID="${1:-dbt_marts_hourly}"

echo "Tasks in $DAG_ID:"
docker compose exec -T scheduler airflow tasks list "$DAG_ID"
echo "---"
COUNT=$(docker compose exec -T scheduler airflow tasks list "$DAG_ID" | grep -c .)
echo "Total task count: $COUNT"