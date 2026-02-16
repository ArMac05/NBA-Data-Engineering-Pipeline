import pandas as pd
import json
import duckdb
from src.silver.schema import Schemas
from src.silver.utils import normalize_json, clean_data, enforce_schema

def test_normalize_json(tmp_path, schema):
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
        "home_in_bonus": None,
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

    json_file = tmp_path / "data.json"
    json_file.write_text(json.dumps(sample))

    root_name = schema["__root__"]
    rows = normalize_json(json_file, root_name)

    assert len(rows) == 1
    row = rows[0]

    assert row["id"] == 18446819
    assert row["home_team_id"] == 21
    assert row["visitor_team_abbreviation"] == "HOU"


def test_clean_data():
    # --- Arrange ---
    df = pd.DataFrame([
        {
            "game_id": 1,
            "status": " Final ",
            "home_team_name": " Thunder ",
            "visitor_team_name": "",
            "home_in_bonus": True,
        },
        {
            "game_id": 1,  # duplicate row
            "status": " Final ",
            "home_team_name": " Thunder ",
            "visitor_team_name": "",
            "home_in_bonus": True,
        }
    ])

    cleaned = clean_data(df)

    # 1. Deduplication
    assert len(cleaned) == 1

    row = cleaned.iloc[0].to_dict()

    # 2. Whitespace trimming
    assert row["status"] == "Final"
    assert row["home_team_name"] == "Thunder"

    # 3. Empty string → None
    assert row["visitor_team_name"] is None

    # 4. cleaned_at exists and is a timestamp
    assert "cleaned_at" in cleaned.columns
    assert pd.api.types.is_datetime64_any_dtype(cleaned["cleaned_at"])

    # 5. Types preserved
    assert isinstance(row["home_in_bonus"], (bool, type(True)))


def test_enforce_schema():
    # --- Arrange ---
    # DataFrame missing some columns and containing extra ones
    df = pd.DataFrame([{
        "game_id": "18446819",   # wrong type (string)
        "date": "2025-10-21",    # string, should become DATE
        "status": "Final",
        "extra_column": "REMOVE_ME"
    }])

    # --- Act ---
    enforced = enforce_schema(df, Schemas.GAMES)

    # --- Assert ---

    # 1. Extra columns removed
    assert "extra_column" not in enforced.columns

    # 2. All schema columns exist
    for col in Schemas.GAMES:
        assert col in enforced.columns

    # 3. Missing columns added as NULL
    assert enforced["visitor_team_division"].isna().all()

    # 4. Column order matches schema order
    assert list(enforced.columns) == list(Schemas.GAMES.keys())

    # 5. Types are correct (DuckDB enforces them)
    rel = duckdb.from_df(enforced)
    dtypes = {name: str(dtype) for name, dtype in zip(rel.columns, rel.dtypes)}

    assert dtypes["game_id"] == "BIGINT"
    assert dtypes["date"] == "DATE"
    assert dtypes["status"] == "VARCHAR"
