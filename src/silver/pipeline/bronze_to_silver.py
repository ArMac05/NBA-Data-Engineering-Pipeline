"""
bronze_to_silver.py

Orchestrates the Bronze to Silver transformation step of the data pipeline.

This module:
    - Scans Bronze directories for raw JSON files
    - Converts JSON file into a cleaned, schema‑enforced Parquet file
      using the json_to_parquet converter
    - Writes outputs into the Silver layer (Batch transformation)
    - Ensures Silver directory structure exists for each endpoint

Notes:
    - ENDPOINTS defines which Bronze folders are processed
    - json_to_parquet handles normalization, cleaning, and schema enforcement
    - This module is typically invoked by an Airflow DAG
"""

from pathlib import Path
from src.silver.schema import Schemas
from src.silver.converters.json_to_parquet import json_to_parquet

BRONZE_BASE = Path("/opt/airflow/data/bronze")
SILVER_BASE = Path("/opt/airflow/data/silver")

ENDPOINTS = ["games", "teams"]
SCHEMAS = {
    "games": Schemas.GAMES,
    "teams": Schemas.TEAMS
}
def bronze_to_silver():
    for endpoint in ENDPOINTS:
        bronze_dir = BRONZE_BASE / endpoint
        silver_dir = SILVER_BASE / endpoint
        silver_dir.mkdir(parents=True, exist_ok=True)

        schema = SCHEMAS[endpoint]

        for json_file in bronze_dir.glob("*.json"):
            parquet_file = silver_dir / f"{json_file.stem}.parquet"

            json_to_parquet(json_file, parquet_file, schema)
            print(f"Converted {json_file} → {parquet_file}")

