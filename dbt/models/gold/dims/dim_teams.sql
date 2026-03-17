{{ config(materialized='table') }}

SELECT 
    team_id,
    conference,
    division,
    city,
    name,
    full_name,
    abbreviation,
    loaded_at
FROM {{ ref('silver_teams') }}
WHERE city IS NOT NULL
