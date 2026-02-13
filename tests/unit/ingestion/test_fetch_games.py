from unittest.mock import patch, MagicMock, mock_open
from pathlib import Path
from src.ingestion.fetch_games import fetch_games

@patch("src.ingestion.fetch_games.Path.mkdir")
@patch("builtins.open", new_callable=mock_open)
@patch("src.ingestion.fetch_games.save_checkpoint")
@patch("src.ingestion.fetch_games.load_checkpoint", return_value=None)
@patch.dict("os.environ", {"API_KEY": "FAKE_KEY"})
@patch("src.ingestion.fetch_games.APIClient")
def test_fetch_games_happy_path(mock_client, mock_load, mock_save, mock_file, mock_mkdir):
    instance = mock_client.return_value
    instance.get.side_effect = [
        {"data": [{"id": 1}], "meta": {"next_cursor": "abc"}},
        {"data": [{"id": 2}], "meta": {"next_cursor": None}},
    ]

    result = fetch_games()

    assert instance.get.call_count == 2
    assert mock_save.call_count == 2

    # Assert that open() was called for the output file
    mock_file.assert_any_call(
        Path("/opt/airflow/data/bronze/games/games.json"), "w"
    )

    assert isinstance(result, str)
