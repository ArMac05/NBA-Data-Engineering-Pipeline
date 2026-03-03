# ------------------------------------------------------------------------------
# dbt-duckdb Runtime Image
#
# This Dockerfile builds a lightweight Python 3.11 container with dbt-core
# and dbt-duckdb installed from PyPI. It is used as the execution environment
# for dbt runs inside the Airflow-based ELT pipeline.
#
# Notes:
#   - Versions are pinned for reproducibility.
#   - No GHCR images are required; everything installs from PyPI.
#   - WORKDIR is set to /usr/app/dbt where dbt projects are mounted.
# ------------------------------------------------------------------------------


FROM python:3.11-slim

# Install git, dbt-core, and dbt-duckdb
RUN apt-get update && apt-get install -y git && \
    pip uninstall -y protobuf || true && \
    pip install --no-cache-dir "protobuf<4.21" dbt-core==1.7.4 dbt-duckdb==1.7.4 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/app/dbt

