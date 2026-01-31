"""
utils.py

Utility functions for ingestion pipelines.

This module provides shared helpers used across all ingestion tasks,
including timestamp generation, directory creation, checkpoint
management, exponential backoff handling, and season detection logic
for NBA data workflows.

Functions:
    utc_timestamp():
        Returns a formatted timestamp in U.S. Pacific Time for
        consistent file naming and event logging.

    ensure_dir(path):
        Safely creates directories for Bronze/Silver/Gold storage layers.

    save_checkpoint(path, value):
        Writes a checkpoint value (cursor, page number, etc.) to disk.

    load_checkpoint(path):
        Reads and returns the stored checkpoint value, or None if missing.

    apply_backoff(retries, max_retries, label):
        Implements capped exponential backoff for retryable API failures.

    get_current_nba_season(today):
        Computes the current NBA season start year based on the calendar.

Notes:
    - These utilities are intentionally lightweight and dependency‑minimal.
    - Designed for use in Airflow, cron jobs, or standalone ingestion scripts.
    - Checkpoints enable safe resumption of long‑running or paginated jobs.
"""


import pytz
import json
import time
from datetime import datetime, date
from pathlib import Path

def utc_timestamp():
    """
    Generate a timestamp string in U.S. Pacific Time (Los Angeles)

    Parameters: None

    Returns:
        str: A formatted timestamp string in the form
        "YYYY-MM-DDTHH-MM-SS", using the American/Los_Angeles timezone.

    Notes:
        - Useful for naming output files or logging ingestion events.
        - uses pytz for reliable timezone handling.
    """
    pacific = pytz.timezone('America/Los_Angeles')
    return datetime.now(pacific).strftime("%Y-%m-%dT%H-%M-%S")

def ensure_dir(path: str):
    """
    Create a directory (and any missing parent directories) at the given path.

    Parameters:
        path (str): Directory path to create.

    Returns: None

    Notes:
        - Safe to call repeatedly; no error is raised if the directory exists.
        - Commonly used for Bronze/Silver/Gold layer folder creation.
    """
    Path(path).mkdir(parents=True, exist_ok=True)

def save_checkpoint(path: str, value):
    """
    Save a checkpoint value to a file.

    Returns: None

    Parameters:
        path (str): File path where the checkpoint will be stored.
        value (Any): THe value to write (cursor, page number, timestamp, etc.)

    Notes
    """
    with open(path, "w") as f:
        f.write(str(value))

def load_checkpoint(path: str):
    """
    Load and return the contents of a checkpoint file.

    Parameters:
        path (str): Path to the checkpoint file.

    Returns:
        str | None: The stored checkpoint value, or None if the file does not exists.

    Notes:
        - Returned value is raw text; callers may need to cast types.
        - Used for cursor-based pagination or incremental ingestion
    """
    if not Path(path).exists():
        return None
    return Path(path).read_text()

def apply_backoff(retries: int, max_retries: int, label: str = "Retrying"):
    """
    Apply exponential backoff when retrying API calls.

    Parameters:
        retries (int): Current retry attempt count.
        max_retries (int): Maximum number of allowed retries.
        label (str): Optional label for logging context.

    Returns:
        int: Updated retry count after applying backoff.

    Raises:
        Exception: If the retry count exceeds `max_retries`.

    Notes:
        - Backoff uses exponential growth (2^retries) capped at 30 seconds.
        - Helps avoid hammering APIs during rate limits or transient failures.
    """
    wait = min(2 ** retries, 30)  # exponential backoff with cap
    print(f"{label}. Waiting {wait} seconds...")
    time.sleep(wait)

    retries += 1
    if retries > max_retries:
        raise Exception(f"Exceeded max retries during {label.lower()}.")

    return retries


def get_current_nba_season(today: date | None = None) -> int:
    """
    Determine the current NBA season start year based on the given date.

    NBA seasons run from October through June.  
    Example:
        - January 2026 → 2025 season  
        - October 2025 → 2025 season  

    Parameters:
        today (date | None): Optional date to evaluate. Defaults to today's date.

    Returns:
        int: The NBA season start year.

    Notes:
        - This logic matches the NBA calendar: season starts in October.
        - Useful for dynamic ingestion pipelines that always target the
          correct season without manual updates.
    """
    if today is None:
        today = date.today()
    
    if today.month >= 10:
        season = today.year
    else:
        season = today.year - 1
    return season
