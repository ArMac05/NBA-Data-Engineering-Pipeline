import json
import duckdb
from src.silver.converters.json_to_parquet import json_to_parquet
from src.silver.schema import Schemas

def test_json_to_parquet(tmp_path):
    # --- Arrange ---
    # Sample JSON input (single game)
    sample = {
        "id": 18446819,
        "date": "2025-10-21",
        "season": 2025,
        "status": "Final",
        "period": 6,
        "time": "Final",
        "postseason": False,
        "postponed": False,
        "home_team_score": 125,
        "visitor_team_score": 124,
        "datetime": "2025-10-21T23:30:00.000Z",
        "home_q1": 27,
        "home_q2": 24,
        "home_q3": 24,
        "home_q4": 29,
        "home_ot1": 11,
        "home_ot2": 10,
        "home_ot3": None,
        "home_timeouts_remaining": 1,
        "home_in_bonus": True,
        "visitor_q1": 30,
        "visitor_q2": 27,
        "visitor_q3": 22,
        "visitor_q4": 25,
        "visitor_ot1": 11,
        "visitor_ot2": 9,
        "visitor_ot3": None,
        "visitor_timeouts_remaining": 0,
        "visitor_in_bonus": False,
        "ist_stage": None,
        "home_team": {
            "id": 21,
            "conference": "West",
            "division": "Northwest",
            "city": "Oklahoma City",
            "name": "Thunder",
            "full_name": "Oklahoma City Thunder",
            "abbreviation": "OKC"
        },
        "visitor_team": {
            "id": 11,
            "conference": "West",
            "division": "Southwest",
            "city": "Houston",
            "name": "Rockets",
            "full_name": "Houston Rockets",
            "abbreviation": "HOU"
        }
    }

    # Write JSON to temp file
    json_file = tmp_path / "input.json"
    json_file.write_text(json.dumps(sample))

    # Output Parquet path
    parquet_file = tmp_path / "output.parquet"

    # --- Act ---
    json_to_parquet(json_file, parquet_file)
    # --- Assert ---
    # 1. Parquet file exists
    assert parquet_file.exists()

    # 2. Load Parquet using DuckDB
    df = duckdb.read_parquet(str(parquet_file)).df()

    # 3. Should have exactly 1 row
    assert len(df) == 1

    # 4. Check a few key fields
    row = df.loc[df.index[0]]

    assert row["game_id"] == 18446819
    assert row["home_team_id"] == 21
    assert row["visitor_team_abbreviation"] == "HOU"

    # 5. Schema enforcement: all expected columns exist
    for col in Schemas.GAMES:
        assert col in df.columns
