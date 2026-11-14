FROM apache/airflow:2.8.1-python3.10

USER root

# Install git, jq, and basic build dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        git \
        jq \
        curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python packages (Airbyte provider + dbt BigQuery adapter)
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

