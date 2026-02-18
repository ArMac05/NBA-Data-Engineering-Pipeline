{{ config(materialized='table') }}

select
    game_id,
    date,
    season,
    status,
    period,
    time,
    home_team_id,
    home_team_score,
    visitor_team_id,
    visitor_team_score,
    datetime,
    home_team_name,
    visitor_team_name,
    cleaned_at as loaded_at
from read_parquet('../data/silver/games/*.parquet')
where date is not null