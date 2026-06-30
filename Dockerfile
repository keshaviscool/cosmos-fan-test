FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends git build-essential \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Separate dbt virtualenv, mirroring the production pattern of keeping dbt's
# dependency tree isolated from Airflow's own.
RUN python -m venv /opt/airflow/dbt_env \
    && /opt/airflow/dbt_env/bin/pip install --no-cache-dir "dbt-postgres>=1.8,<1.9"