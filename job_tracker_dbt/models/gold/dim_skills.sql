WITH unique_skills AS (
    SELECT DISTINCT
        skill_name,
        skill_category
    FROM {{ ref('silver_job_skills') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['skill_name']) }} AS skill_id,
    skill_category,
    skill_name
FROM unique_skills
