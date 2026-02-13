from datetime import datetime, date
from src.ingestion.utils import get_current_nba_season


def test_detect_season():
    assert get_current_nba_season(date(2025, 12, 1)) == 2025

def test_detect_season_edge_case():
    assert get_current_nba_season(date(2025, 10, 1)) == 2025

def test_detect_season_empty_param():
    assert get_current_nba_season() == 2025