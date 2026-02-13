import duckdb  # type: ignore
import pandas as pd
from src.silver.schema import Schemas
from src.silver.utils import normalize_json, clean_data, enforce_schema

def json_to_parquet(input_json_path, output_parquet_path):
    """
    General-purpose JSON → Parquet converter.
    - Reads a JSON file
    - Normalizes nested objects into rows
    - Cleans data
    - Enforces schema
    - Writes Parquet using DuckDB
    """

    # Normalize JSON → list of dicts
    rows = normalize_json(input_json_path)

    # Convert list → DataFrame
    df = pd.DataFrame(rows)

    # Clean data
    df = clean_data(df)

    # Enforce schema
    df = enforce_schema(df, Schemas.GAMES)

    # Write Parquet using DuckDB
    duckdb.from_df(df).write_parquet(str(output_parquet_path))

    return output_parquet_path
