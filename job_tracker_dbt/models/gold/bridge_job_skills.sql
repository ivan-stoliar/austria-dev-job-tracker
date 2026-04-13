SELECT
    job_id,
    {{ dbt_utils.generate_surrogate_key(['skill_name']) }} AS skill_id
FROM {{ ref('silver_job_skills') }}
