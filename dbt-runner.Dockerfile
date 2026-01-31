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

# Install dbt-core and dbt-duckdb from PyPI (no GHCR needed)
RUN pip install --no-cache-dir dbt-core==1.7.4 dbt-duckdb==1.7.4

WORKDIR /usr/app/dbt
