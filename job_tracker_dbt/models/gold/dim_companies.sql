WITH unique_companies AS (
    SELECT DISTINCT company_name
    FROM {{ ref('silver_jobs') }}
    WHERE company_name IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['company_name']) }} AS company_id,
    company_name
FROM unique_companies
