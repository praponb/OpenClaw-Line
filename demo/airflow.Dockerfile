# Airflow plus the Databricks provider (for dags/databricks_daily_revenue.py).
# Pinned with Airflow's constraints file for this exact Airflow/Python version.
ARG AIRFLOW_IMAGE=apache/airflow:3.3.2
FROM ${AIRFLOW_IMAGE}
RUN PY="$(python -c 'import sys; print(f"{sys.version_info[0]}.{sys.version_info[1]}")')" \
 && pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" apache-airflow-providers-databricks \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PY}.txt"
