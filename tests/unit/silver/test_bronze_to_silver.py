import json
import duckdb
from src.silver.pipeline.bronze_to_silver import bronze_to_silver

def test_bronze_to_silver(tmp_path, monkeypatch):
    # create fake bronze directory
    bronze_games = tmp_path / "bronze" / "games"
    bronze_games.mkdir(parents=True)

    data = {"id": 1, "team": {"name": "Lakers"}}
    json_file = bronze_games / "game1.json"
    with open(json_file, "w") as f:
        json.dump(data, f)

    # patch BRONZE_BASE and SILVER_BASE inside the module
    monkeypatch.setattr("src.silver.pipeline.bronze_to_silver.BRONZE_BASE", tmp_path / "bronze")
    monkeypatch.setattr("src.silver.pipeline.bronze_to_silver.SILVER_BASE", tmp_path / "silver")

    bronze_to_silver()

    # parquet file exists in silver
    silver_games = tmp_path / "silver" / "games"
    files = list(silver_games.glob("*.parquet"))
    assert len(files) == 1

    df = duckdb.read_parquet(str(files[0])).df()
    rows = df.to_dict(orient="records")
    assert rows[0]["team.name"] == "Lakers"