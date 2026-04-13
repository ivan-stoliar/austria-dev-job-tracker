WITH date_spine AS (

    SELECT CAST(d AS DATE) AS date_day
    FROM generate_series(
        '2024-01-01'::DATE,
        '2026-12-31'::DATE,
        '1 day'::interval
    ) AS d
)

SELECT
    -- integer ID
    TO_CHAR(date_day, 'YYYYMMDD')::INT AS date_id,
    date_day AS full_date,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(MONTH FROM date_day) AS month,
    TO_CHAR(date_day, 'Month') AS month_name,
    EXTRACT(QUARTER FROM date_day) AS quarter,
    EXTRACT(ISODOW FROM date_day) AS day_of_week,

    CASE WHEN EXTRACT(ISODOW FROM date_day) IN (6, 7) THEN true ELSE false END AS is_weekend
FROM date_spine
