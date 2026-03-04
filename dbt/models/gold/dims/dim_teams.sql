{{ config(materialized='table') }}

SELECT 
    team_id,
    conference,
    division,
    city,
    name,
    full_name,
    abbreviation,
    cleaned_at AS loaded_at
FROM read_parquet('../data/silver/teams/*.parquet')
WHERE team_id IS NOT NULL
