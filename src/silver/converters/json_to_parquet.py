"""
json_to_parquet.py

Silver-layer transformation utility for converting normalized Bronze JSON
into schema-enforced Parquet files.

Responsibilities:
    - Read raw or normalized JSON from Bronze storage
    - Flatten nested structures into row dictionaries
    - Clean and standardize values for Silver
    - Enforce the Silver-layer schema (column order, dtypes)
    - Write Parquet output using DuckDB for consistent, fast I/O

Notes:
    - This module is used by bronze_to_silver.py and Airflow DAGs
    - Schemas are defined in src/silver/schema.py
    - Cleaning and normalization utilities live in src/silver/utils.py
"""

import duckdb  # type: ignore
import pandas as pd
from src.silver.schema import Schemas
from src.silver.utils import normalize_json, clean_data, enforce_schema

def json_to_parquet(input_json_path, output_parquet_path, schema: dict):
    """
    General-purpose JSON → Parquet converter.
    - Reads a JSON file
    - Normalizes nested objects into rows
    - Cleans data
    - Enforces schema
    - Writes Parquet using DuckDB
    """
    # Extract root name
    root_name = schema["__root__"]

    # Normalize JSON
    rows = normalize_json(input_json_path, root_name)
    df = pd.DataFrame(rows)

    # Clean
    df = clean_data(df)

    # Remove __root__ before schema enforcement
    schema_no_root = {k: v for k, v in schema.items() if k != "__root__"}

    # Enforce schema
    df = enforce_schema(df, schema_no_root)

    # Write Parquet
    duckdb.from_df(df).write_parquet(str(output_parquet_path))


    return output_parquet_path
