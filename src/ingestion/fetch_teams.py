"""
fetch_teams

Teams Ingestion Module

Fetches all NBA teams from the BallDontLie API and writes the raw,
unmodified response into the Bronze layer. This endpoint does not use
pagination, so the ingestion is performed in a single request.

Responsibilities:
- Load API key from environment
- Initialize shared APIClient
- Request /teams endpoint
- Persist raw JSON to /data/bronze/teams_raw.json

This module is part of the ingestion layer for the NBA data pipeline.
"""

import os
import json
from dotenv import load_dotenv
from pathlib import Path
from src.ingestion.base_client import APIClient

CHECKPOINT_PATH = "/opt/airflow/data/checkpoints/cursor_teams.txt"
OUTPUT_PATH = "/opt/airflow/data/raw/teams/teams.json"

def fetch_teams():
    load_dotenv()
    api_key = os.getenv("API_KEY")
    client = APIClient(
        base_url="https://api.balldontlie.io/v1",
        api_key=api_key
    )

    # Fetch teams from the API
    data = client.get("teams")

    # Save to bronze layer
    output_path = Path(OUTPUT_PATH)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Save raw JSON exactly as received
    with open(output_path, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Saved {len(data.get('data', []))} teams to {output_path}")
    return str(output_path)


if __name__ == "__main__":
    fetch_teams()
