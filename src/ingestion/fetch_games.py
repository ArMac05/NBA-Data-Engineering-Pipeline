"""
fetch_games.py

Ingestion module for NBA game data using the BallDontLie API.

This module fetches all games for the current NBA season using
cursor-based pagination, persists raw responses to the Bronze layer,
and maintains a checkpoint file to support safe, incremental ingestion.

Responsibilities:
    - Load API credentials from environment variables
    - Determine the current NBA season dynamically
    - Fetch paginated game data from the BallDontLie API
    - Track progress using a cursor checkpoint
    - Write raw JSON output to the Bronze storage path

Notes:
    - The Bronze layer stores unmodified API responses for reproducibility.
    - Checkpointing ensures the ingestion job can resume after interruptions.
    - This module is designed for Airflow or other orchestrated pipelines.
"""


import json
import os
from datetime import date
from pathlib import Path
from dotenv import load_dotenv
from .base_client import APIClient
from .utils import load_checkpoint, save_checkpoint, get_current_nba_season

CHECKPOINT_PATH = "/opt/airflow/data/bronze/cursor_games.txt"
OUTPUT_PATH = "/opt/airflow/data/bronze/games_raw.json"

def fetch_games():
    load_dotenv()
    api_key = os.getenv("API_KEY")

    if not api_key: 
        raise ValueError("API_KEY not found in environment variables.")

    client = APIClient(
        base_url="https://api.balldontlie.io/v1",
        api_key=api_key
    )
    
    season = get_current_nba_season()
    print(f"Fetching games for season {season}...")
    
    cursor = load_checkpoint(CHECKPOINT_PATH)
    all_games = []

    while True:
        params = {
            "seasons[]": [season],
            "end_date": str(date.today()),
            "per_page": 100
        }
        if cursor:
            params["cursor"] = cursor

        page = client.get("games", params=params)

        # Safety check 
        if not page.get("data"): 
            break

        all_games.extend(page["data"])

        cursor = page["meta"].get("next_cursor")
        save_checkpoint(CHECKPOINT_PATH, cursor or "")

        print(f"Fetched {len(page['data'])} games... total so far: {len(all_games)}")

        if cursor is None:
            break
    
    # Save to bronze layer
    output_path = Path(OUTPUT_PATH)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(all_games, f, indent=2)

    print(f"Saved {len(all_games)} total games for season {season} to {output_path}")
    return str(output_path)

if __name__ == "__main__":
    fetch_games()