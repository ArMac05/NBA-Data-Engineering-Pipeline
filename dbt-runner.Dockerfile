FROM python:3.11-slim

# Install dbt-core and dbt-duckdb from PyPI (no GHCR needed)
RUN pip install --no-cache-dir dbt-core==1.7.4 dbt-duckdb==1.7.4

WORKDIR /usr/app/dbt
