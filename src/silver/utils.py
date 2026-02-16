"""
utils.py

Utility functions for Silver-layer transformations.

This module provides:
    - JSON normalization (flattening nested API responses)
    - Data cleaning (whitespace trimming, null handling, deduplication)
    - Schema enforcement using DuckDB (casting, ordering, missing columns)

These functions are used by:
    - json_to_parquet.py
    - bronze_to_silver.py
    - Airflow DAGs orchestrating Bronze → Silver processing

Notes:
    - normalize_json handles multiple API response shapes
    - clean_data adds a UTC 'cleaned_at' timestamp for lineage
    - enforce_schema ensures strict Silver-layer typing and column order
"""

import pandas as pd
import duckdb
import json

def normalize_json(json_file_path, root_name: str):
    """
    General-purpose JSON normalizer.
    Handles:
    - A single JSON object
    - A list of JSON objects
    - API responses with {"data": [...]}
    - Nested objects (flattened with sep="_")
    Returns a list of flat dictionaries.
    """
    with open(json_file_path, "r") as f:
        raw = json.load(f)

    if isinstance(raw, dict) and "data" in raw:
        records = raw["data"]
    elif isinstance(raw, list):
        records = raw
    else:
        records = [raw]

    df = pd.json_normalize(records, sep="_", max_level=1)

    # Correct: rename id → <root>_id
    if "id" in df.columns:
        df = df.rename(columns={"id": f"{root_name}_id"})

    records = df.to_dict(orient="records")

    cleaned_records = [
        {k: (None if pd.isna(v) else v) for k, v in row.items()}
        for row in records
    ]

    return cleaned_records



def clean_data(df: pd.DataFrame):
    """
    Clean data for silver layer using pandas
    - Removes leading and trailing white spaces
    - Removes duplicates
    - Add time stamp of when data was cleaned
    """
    # Trim whitespaces
    str_cols = df.select_dtypes(include=["object", "string"]).columns 
    df[str_cols] = df[str_cols].apply(lambda col: col.str.strip())

    # Convert empty strings to None
    df = df.replace({"": None})

    # Convert pandas NA to Python None (critical)
    df = df.mask(pd.isna(df), None)

    # Drop duplicates 
    df = df.drop_duplicates()

    # Metadata
    df["cleaned_at"] = pd.Timestamp.utcnow()
    return df



def enforce_schema(df, schema: dict):
    """
    Enforce a strict Silver-layer schema using DuckDB.
    - casts columns
    - adds missing columns
    - removes extra columns
    - orders columns
    """

    # Add missing columns as NULL
    for col in schema.keys():
        if col not in df.columns:
            df[col] = None

    df = df.astype(object)

    # Register the DataFrame as a DuckDB view
    duckdb.register("tmp_df", df)

    # Build SELECT with casts
    select_expr = ",\n".join([
        f"CAST({col} AS {dtype}) AS {col}"
        for col, dtype in schema.items()
    ])

    # Run the schema enforcement
    enforced = duckdb.sql(f"""
        SELECT {select_expr}
        FROM tmp_df
    """).df()

    enforced = enforced.astype(object)

    # Convert DATE columns from TIMESTAMP back to date type
    for col, dtype in schema.items():
        if dtype == "DATE" and col in enforced.columns:
            enforced[col] = pd.to_datetime(enforced[col]).dt.date

    return enforced
