-- Build the model as a physical table
-- Each dbt run will recreate the table
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
from read_parquet('../data/silver/teams/*.parquet')
where team_id is not null