{{ config(materialized='table') }}

SELECT
    id AS game_id,
    date::date AS date,
    season,
    status,
    period,
    time,
    postseason,
    postponed,
    home_team_score,
    visitor_team_score,
    datetime::timestamp AS datetime,

    -- Quarter scoring
    home_q1, home_q2, home_q3, home_q4,
    visitor_q1, visitor_q2, visitor_q3, visitor_q4,

    -- Overtime scoring
    home_ot1, home_ot2, home_ot3,
    visitor_ot1, visitor_ot2, visitor_ot3,

    -- Timeout & bonus
    home_timeouts_remaining,
    visitor_timeouts_remaining,
    home_in_bonus,
    visitor_in_bonus,

    -- Team metadata (flattened)
    home_team.id AS home_team_id,
    home_team.conference AS home_conference,
    home_team.division AS home_division,
    home_team.city AS home_city,
    home_team.name AS home_name,
    home_team.full_name AS home_full_name,
    home_team.abbreviation AS home_abbreviation,

    visitor_team.id AS visitor_team_id,
    visitor_team.conference AS visitor_conference,
    visitor_team.division AS visitor_division,
    visitor_team.city AS visitor_city,
    visitor_team.name AS visitor_name,
    visitor_team.full_name AS visitor_full_name,
    visitor_team.abbreviation AS visitor_abbreviation,

    now() AS loaded_at

FROM read_json('../data/bronze/games/*.json')
