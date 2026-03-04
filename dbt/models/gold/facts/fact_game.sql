{{ config(materialized='table') }}

WITH base AS (
    SELECT
        game_id,
        date,
        datetime,
        season,
        status,
        period,
        postseason,
        postponed,
        home_team_id,
        visitor_team_id,
        home_team_score,
        visitor_team_score,

        home_q1, home_q2, home_q3, home_q4,
        visitor_q1, visitor_q2, visitor_q3, visitor_q4,

        home_ot1, home_ot2, home_ot3,
        visitor_ot1, visitor_ot2, visitor_ot3
    FROM {{ ref('silver_games') }}
)

SELECT
    *,
    home_team_score + visitor_team_score AS total_points,
    ABS(home_team_score - visitor_team_score) AS point_diff,
    CASE WHEN home_team_score > visitor_team_score THEN home_team_id ELSE visitor_team_id END AS winner_team_id,
    CASE WHEN home_team_score < visitor_team_score THEN home_team_id ELSE visitor_team_id END AS loser_team_id,
    CASE WHEN period > 4 THEN TRUE ELSE FALSE END AS went_to_overtime,
    (period - 4) AS num_overtimes
FROM base
