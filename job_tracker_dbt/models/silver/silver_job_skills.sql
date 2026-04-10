{{ config(materialized='table') }}

WITH clean_jobs AS (
    SELECT
        job_id,
        CONCAT(job_title, ' ', job_description) AS searchable_text
    FROM {{ ref('silver_jobs') }}
    WHERE job_description IS NOT NULL
      AND job_description != ''
),

taxonomy AS (
    SELECT
        category,
        standard_name,
        regex_pattern
    FROM {{ ref('taxonomy') }}
),

extracted_skills AS (
    SELECT
        j.job_id,
        t.category AS skill_category,
        t.standard_name AS skill_name
    FROM clean_jobs j
    INNER JOIN taxonomy t
        ON j.searchable_text ~* t.regex_pattern
)

SELECT
    job_id,
    skill_category,
    skill_name
FROM extracted_skills
GROUP BY
    job_id,
    skill_category,
    skill_name
