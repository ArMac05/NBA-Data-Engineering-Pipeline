import pytest
import requests
from unittest.mock import patch
from src.ingestion.base_client import APIClient

@patch("requests.get")
def test_get_constructs_correct_url(mock_get):
    """
    Test 1: URL is constructed correctly
    """
    client = APIClient("https://api.test.com", "KEY123")

    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {"ok": True}

    client.get("games")

    mock_get.assert_called_once()
    called_url = mock_get.call_args[0][0]

    assert called_url == "https://api.test.com/games"



@patch("requests.get")
def test_get_includes_api_key(mock_get):
    """
    Test 2: API key is added to header
    """
    client = APIClient("https://api.test.com", "SECRET")

    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {}

    client.get("players")

    headers = mock_get.call_args[1]["headers"]
    assert headers["Authorization"] == "Bearer SECRET"



@patch("time.sleep", return_value=None)
@patch("requests.get")
def test_retry_on_429(mock_get, mock_sleep):
    """
    Test 3: Retry logic triggers on 429

    First call returns 429, second call returns success
    """
    mock_get.side_effect = [
        type("Resp", (), {"status_code": 429, "json": lambda: {}}),
        type("Resp", (), {"status_code": 200, "json": lambda: {"ok": True}})
    ]

    client = APIClient("https://api.test.com", "KEY")

    result = client.get("games")

    assert result == {"ok": True}
    assert mock_get.call_count == 2
    assert mock_sleep.called



@patch("time.sleep", return_value=None)
@patch("requests.get")
def test_retry_exhaustion_raises(mock_get, mock_sleep):
    """
    Test 4: Raise exceptin after max retries
    """
    mock_get.return_value.status_code = 500

    client = APIClient("https://api.test.com", "KEY")

    with pytest.raises(Exception):
        client.get("games")

    assert mock_get.call_count > 1


@patch("requests.get")
def test_get_returns_json(mock_get):
    """
    Test 5: JSON parsing works
    """
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {"data": [1, 2, 3]}

    client = APIClient("https://api.test.com")

    result = client.get("stats")

    assert result == {"data": [1, 2, 3]}


@patch("requests.get", side_effect=requests.exceptions.Timeout)
def test_timeout_raises_clean_error(mock_get):
    """
    Test 6: Handle network errors
    """
    client = APIClient("https://api.test.com")

    with pytest.raises(Exception):
        client.get("games")

