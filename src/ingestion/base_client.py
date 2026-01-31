"""
API client wrapper for ingestion pipelines.

This module provides a lightweight HTTP client with built‑in retry logic,
exponential backoff, and structured handling of rate limits and server
errors. It is designed for use across ingestion tasks that interact with
the BallDontLie API or similar REST endpoints.

Responsibilities:
    - Attach API authentication headers
    - Perform GET requests with timeout protection
    - Retry on rate limits (429) using exponential backoff
    - Retry on transient server errors (500, 502, 503, 504)
    - Fail fast on non‑retryable client errors (4xx)
    - Return parsed JSON responses for successful requests

Notes:
    - This client is intentionally minimal and stateless.
    - Retry behavior is controlled via `apply_backoff` from utils.
    - Suitable for Airflow tasks, cron jobs, or standalone ingestion scripts.
"""


import requests
from typing import Optional
from src.ingestion.utils import apply_backoff


class APIClient:
    # Constructor
    def __init__(self, base_url: str, api_key: Optional[str] = None, headers: Optional[dict] = None, rate_limit: int = 1):
        self.base_url = base_url
        self.api_key = api_key
        self.rate_limit = rate_limit
        self.headers = {"Authorization": f"Bearer {self.api_key}"} 

    def get(self, endpoint: str, params: Optional[dict] = None, max_retries: int = 6):
        url = f"{self.base_url}/{endpoint}"
        retries = 0

        while True:
            response = requests.get(url, headers=self.headers, params=params, timeout=10)

            # Success
            if response.status_code == 200:
                return response.json()

            # Rate limit
            if response.status_code == 429:
                retries = apply_backoff(retries, max_retries, label="Rate limit hit")
                continue

            # Retryable server errors
            if response.status_code in {500, 502, 503, 504}:
                retries = apply_backoff(retries, max_retries, label="Server errors")
                continue

            # Non-retryable client errors
            if 400 <= response.status_code < 500:
                raise Exception(f"Client error {response.status_code}: {response.text}")

            # Anything else
            raise Exception(f"Unexpected error {response.status_code}: {response.text}")
