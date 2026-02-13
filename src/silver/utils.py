import pandas as pd
import duckdb
import json


def normalize_json(json_file_path):
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

    # Case 1: API response with "data"
    if isinstance(raw, dict) and "data" in raw:
        records = raw["data"]

    # Case 2: List of objects
    elif isinstance(raw, list):
        records = raw

    # Case 3: Single object
    else:
        records = [raw]

    # Flatten nested objects
    df = pd.json_normalize(records, sep="_", max_level=1)

    # Rename "id" → "<root>_id" only if needed
    if "id" in df.columns:
        df = df.rename(columns={"id": "id"})

    return df.to_dict(orient="records")


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

    # Convert empty strings to none
    df = df.replace({"": None})

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

    # Convert DATE columns from TIMESTAMP back to date type
    for col, dtype in schema.items():
        if dtype == "DATE" and col in enforced.columns:
            enforced[col] = pd.to_datetime(enforced[col]).dt.date

    return enforced
