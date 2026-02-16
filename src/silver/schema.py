"""
schema.py

Defines strict Silver-layer schemas for normalized NBA data.

This module:
    - Provides explicit column names and DuckDB-friendly data types
    - Serves as the single source of truth for Silver-layer tables
    - Ensures downstream transformations (json_to_parquet, bronze_to_silver)
      enforce consistent structure and typing
    - Supports reproducible analytics and dbt modeling

Notes:
    - GAMES_SCHEMA corresponds to the flattened, cleaned structure of the
      BallDontLie 'games' endpoint
    - Additional schemas can be added as needed
"""

GAMES_SCHEMA = {
    "__root__": "game",
    "game_id": "BIGINT",
    "date": "DATE",
    "season": "INTEGER",
    "status": "VARCHAR",
    "period": "INTEGER",
    "time": "VARCHAR",
    "postseason": "BOOLEAN",
    "postponed": "BOOLEAN",
    "home_team_score": "INTEGER",
    "visitor_team_score": "INTEGER",
    "datetime": "TIMESTAMP",

    # Quarter scoring
    "home_q1": "INTEGER",
    "home_q2": "INTEGER",
    "home_q3": "INTEGER",
    "home_q4": "INTEGER",
    "home_ot1": "INTEGER",
    "home_ot2": "INTEGER",
    "home_ot3": "INTEGER",

    "visitor_q1": "INTEGER",
    "visitor_q2": "INTEGER",
    "visitor_q3": "INTEGER",
    "visitor_q4": "INTEGER",
    "visitor_ot1": "INTEGER",
    "visitor_ot2": "INTEGER",
    "visitor_ot3": "INTEGER",

    # Bonus + timeouts
    "home_timeouts_remaining": "INTEGER",
    "home_in_bonus": "BOOLEAN",
    "visitor_timeouts_remaining": "INTEGER",
    "visitor_in_bonus": "BOOLEAN",

    # IST stage (in-season tournament)
    "ist_stage": "VARCHAR",

    # Flattened home team
    "home_team_id": "BIGINT",
    "home_team_conference": "VARCHAR",
    "home_team_division": "VARCHAR",
    "home_team_city": "VARCHAR",
    "home_team_name": "VARCHAR",
    "home_team_full_name": "VARCHAR",
    "home_team_abbreviation": "VARCHAR",

    # Flattened visitor team
    "visitor_team_id": "BIGINT",
    "visitor_team_conference": "VARCHAR",
    "visitor_team_division": "VARCHAR",
    "visitor_team_city": "VARCHAR",
    "visitor_team_name": "VARCHAR",
    "visitor_team_full_name": "VARCHAR",
    "visitor_team_abbreviation": "VARCHAR",

    # Metadata
    "cleaned_at": "TIMESTAMP"
}

TEAMS_SCHEMA = {
    "__root__": "team",
    "team_id": "BIGINT",
    "conference": "VARCHAR",
    "division": "VARCHAR",
    "city": "VARCHAR",
    "name": "VARCHAR",
    "full_name": "VARCHAR",
    "abbreviation": "VARCHAR"
}

class Schemas:
    GAMES = GAMES_SCHEMA
    TEAMS = TEAMS_SCHEMA