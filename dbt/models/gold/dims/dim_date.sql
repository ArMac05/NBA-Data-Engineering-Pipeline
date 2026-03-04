{{ config(materialized='table') }}

WITH dates AS (
    SELECT
        d::date AS date
    FROM range(date '2010-01-01', date '2035-12-31', interval 1 day) AS t(d)
),

enhanced AS (
    SELECT
        date,

        -- Basic calendar fields
        EXTRACT(year FROM date) AS year,
        EXTRACT(month FROM date) AS month,
        EXTRACT(day FROM date) AS day,
        EXTRACT(quarter FROM date) AS quarter,
        EXTRACT(week FROM date) AS week_of_year,

        -- Names using DuckDB strftime()
        strftime(date, '%B') AS month_name,
        strftime(date, '%A') AS day_name,
        EXTRACT(dow FROM date) AS day_of_week,

        -- Weekend flag
        CASE WHEN EXTRACT(dow FROM date) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,

        -- Year-month string
        strftime(date, '%Y-%m') AS year_month,

        -- NBA season logic (season starts in October)
        CASE 
            WHEN EXTRACT(month FROM date) >= 10 THEN EXTRACT(year FROM date)
            ELSE EXTRACT(year FROM date) - 1
        END AS season,

        -- Season month index (Oct = 1, Nov = 2, ..., Jun = 9)
        CASE 
            WHEN EXTRACT(month FROM date) >= 10 THEN EXTRACT(month FROM date) - 9
            ELSE EXTRACT(month FROM date) + 3
        END AS season_month_index,

        -- NBA calendar flags
        CASE 
            WHEN EXTRACT(month FROM date) BETWEEN 10 AND 4 THEN TRUE
            ELSE FALSE
        END AS is_regular_season,

        CASE 
            WHEN EXTRACT(month FROM date) BETWEEN 5 AND 6 THEN TRUE
            ELSE FALSE
        END AS is_playoffs,

        CASE 
            WHEN EXTRACT(month FROM date) BETWEEN 8 AND 9 THEN TRUE
            ELSE FALSE
        END AS is_preseason

    FROM dates
)

SELECT * FROM enhanced
