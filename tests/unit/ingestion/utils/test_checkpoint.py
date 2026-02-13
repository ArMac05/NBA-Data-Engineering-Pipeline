from src.ingestion.utils import save_checkpoint, load_checkpoint

def test_save_checkpoint_writes_value(tmp_path):
    """
    Test: save_checkpoint writes the correct value
    """
    file_path = tmp_path / "games_cursor.json"

    save_checkpoint(file_path, "cursor123")

    assert file_path.exists()
    assert file_path.read_text() == "cursor123"

def test_load_checkpoint_read_value(tmp_path):
    """
    Test: load_checkpoint returns correct value
    """
    file_path = tmp_path / "cursor.json"
    file_path.write_text("xyz789")

    result = load_checkpoint(file_path)

    assert result == "xyz789"

def test_load_checkpoint_missing_file(tmp_path):
    file_path = tmp_path / "does_not_exist.json"

    result = load_checkpoint(file_path)

    assert result is None
