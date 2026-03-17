{{ config(materialized='table') }}

WITH base AS (
    SELECT
        game_id,
        date,
        season,
        home_team_id,
        home_name,
        visitor_team_id,
        visitor_name,
        home_team_score,
        visitor_team_score,

        home_q1, home_q2, home_q3, home_q4,
        visitor_q1, visitor_q2, visitor_q3, visitor_q4,

        home_ot1, home_ot2, home_ot3,
        visitor_ot1, visitor_ot2, visitor_ot3
    FROM {{ ref('silver_games') }}
),

home_rows AS (
    SELECT
        game_id,
        date,
        season,
        home_team_id AS team_id,
        home_name AS name,
        visitor_team_id AS opponent_team_id,
        TRUE AS is_home,

        home_team_score AS points_scored,
        visitor_team_score AS points_allowed,
        home_team_score - visitor_team_score AS point_diff,
        CASE WHEN home_team_score > visitor_team_score THEN 1 ELSE 0 END AS win_flag,

        -- Quarter scoring
        home_q1 AS q1,
        home_q2 AS q2,
        home_q3 AS q3,
        home_q4 AS q4,

        visitor_q1 AS op_q1,
        visitor_q2 AS op_q2,
        visitor_q3 AS op_q3,
        visitor_q4 AS op_q4,

        -- OT scoring
        home_ot1 AS ot1,
        home_ot2 AS ot2,
        home_ot3 AS ot3,

        visitor_ot1 AS op_ot1,
        visitor_ot2 AS op_ot2,
        visitor_ot3 AS op_ot3
    FROM base
),

visitor_rows AS (
    SELECT
        game_id,
        date,
        season,
        visitor_team_id AS team_id,
        visitor_name AS name,
        home_team_id AS opponent_team_id,
        FALSE AS is_home,

        visitor_team_score AS points_scored,
        home_team_score AS points_allowed,
        visitor_team_score - home_team_score AS point_diff,
        CASE WHEN visitor_team_score > home_team_score THEN 1 ELSE 0 END AS win_flag,

        -- Quarter scoring
        visitor_q1 AS q1,
        visitor_q2 AS q2,
        visitor_q3 AS q3,
        visitor_q4 AS q4,

        home_q1 AS op_q1,
        home_q2 AS op_q2,
        home_q3 AS op_q3,
        home_q4 AS op_q4,

        -- OT scoring
        visitor_ot1 AS ot1,
        visitor_ot2 AS ot2,
        visitor_ot3 AS ot3,

        home_ot1 AS op_ot1,
        home_ot2 AS op_ot2,
        home_ot3 AS op_ot3
    FROM base
)

SELECT * FROM home_rows
UNION ALL
SELECT * FROM visitor_rows
