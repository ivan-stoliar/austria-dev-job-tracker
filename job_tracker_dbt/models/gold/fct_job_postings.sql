SELECT
    job_id,
    {{ dbt_utils.generate_surrogate_key(['company_name']) }} AS company_id,

    TO_CHAR(job_posted_at_utc, 'YYYYMMDD')::INT AS date_id,

    job_posted_at_utc,

    job_country as country,
    job_city as city,
    source as platform_name,

    position_level,
    employment_type,
    is_remote,

    salary_min_yearly,
    salary_max_yearly,


    CASE
        WHEN salary_min_yearly IS NOT NULL AND salary_max_yearly IS NOT NULL
        THEN (salary_min_yearly + salary_max_yearly) / 2
    END AS salary_midpoint_yearly

FROM {{ ref('silver_jobs') }}

WHERE description_quality_tier != 'Tier 3 - Missing Description'
