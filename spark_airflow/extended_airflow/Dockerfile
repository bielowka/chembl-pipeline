FROM apache/airflow:2.7.1-python3.8

USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-11-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-2.7.1/constraints-3.8.txt"

RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark \
    rdkit \
    --constraint "${CONSTRAINT_URL}"