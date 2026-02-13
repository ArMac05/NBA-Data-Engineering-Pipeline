from pathlib import Path
from datetime import datetime
from src.silver.converters.json_to_parquet import json_to_parquet

BRONZE_BASE = Path("/opt/airflow/data/bronze")
SILVER_BASE = Path("/opt/airflow/data/silver")

ENDPOINTS = ["games", "teams"]

def bronze_to_silver():
    for endpoint in ENDPOINTS:
        bronze_dir = BRONZE_BASE / endpoint
        silver_dir = SILVER_BASE / endpoint
        silver_dir.mkdir(parents=True, exist_ok=True)

        for json_file in bronze_dir.glob("*.json"):
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            parquet_file = silver_dir / f"{json_file.stem}_{timestamp}.parquet"

            json_to_parquet(json_file, parquet_file)
            print(f"Converted {json_file} → {parquet_file}")
