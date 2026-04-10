{{ config(materialized='table') }}

WITH extracted_jobs_base AS (
    SELECT
        job_id,
        source,
        keyword,
        extracted_at,
        raw_data,

        (raw_data->>'title')::VARCHAR AS job_title,

        CASE
            WHEN source = 'adzuna' THEN (raw_data->'company'->>'display_name')::VARCHAR
            WHEN source = 'jooble' THEN (raw_data->>'company')::VARCHAR
            ELSE (raw_data->>'company_name')::VARCHAR
        END AS company_name,


        REGEXP_REPLACE(
            REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        CASE
                            WHEN source = 'jooble' THEN (raw_data->>'snippet')::TEXT
                            ELSE (raw_data->>'description')::TEXT
                        END,
                        '<[^>]+>', ' ', 'g'
                    ),
                    '[\r\n\t]+', ' ', 'g'
                ),
                '&nbsp;', ' '
            ),
            '\s{2,}', ' ', 'g'
        ) AS job_description,

        -- Finding country location
        CASE
            WHEN source = 'adzuna' THEN
                CASE
                    WHEN (raw_data->'location'->'area'->>0) ~* '(austria|österreich|osterreich|\yat\y)' THEN 'AT'
                    WHEN (raw_data->'location'->'area'->>0) ~* '(germany|deutschland|\yde\y)' THEN 'DE'
                    WHEN (raw_data->'location'->'area'->>0) ~* '(switzerland|schweiz|suisse|\ych\y)' THEN 'CH'
                    ELSE (raw_data->'location'->'area'->>0)::TEXT
                END

        -- Jooble api mostly hides the true location and shows it inside the 'snippet' (description)
            WHEN source = 'jooble' THEN
                CASE
                    -- Catch the fake locations
                    WHEN (raw_data->>'snippet') ~* '(india|bangalore|hyderabad|pune|mumbai|\bin\b)' THEN 'IN'
                    WHEN (raw_data->>'snippet') ~* '(usa|texas|\btx\b|houston|new york|\bny\b|california|\bca\b)' THEN 'US'

                    -- Check snippet for DACH countries AND major DACH cities
                    WHEN (raw_data->>'snippet') ~* '(austria|österreich|osterreich|\yat\y|vienna|wien|graz|linz|salzburg|innsbruck)' THEN 'AT'
                    WHEN (raw_data->>'snippet') ~* '(germany|deutschland|\yde\y|berlin|munich|münchen|hamburg|frankfurt|stuttgart)' THEN 'DE'
                    WHEN (raw_data->>'snippet') ~* '(switzerland|schweiz|suisse|\ych\y|zurich|zürich|geneva|genf|basel|bern)' THEN 'CH'
                    ELSE 'Unknown'
                END

            -- Arbeitnow shows city instead of country
            WHEN source = 'arbeitnow' THEN
                CASE
                    WHEN (raw_data->>'location') ~* '(austria|österreich|osterreich|\yat\y|vienna|wien|graz|linz|salzburg|innsbruck)' THEN 'AT'
                    WHEN (raw_data->>'location') ~* '(germany|deutschland|\yde\y|berlin|munich|münchen|hamburg|frankfurt|stuttgart|leipzig|cologne)' THEN 'DE'
                    WHEN (raw_data->>'location') ~* '(switzerland|schweiz|suisse|\ych\y|zurich|zürich|geneva|genf|basel|bern)' THEN 'CH'
                    ELSE 'Unknown'
                END

            ELSE 'Unknown'

        END AS job_country,

        -- Finding city location
        CASE

            WHEN source = 'adzuna' THEN
                CASE
                    -- Adzuna returns array with nested locations, where last is the city
                    WHEN (raw_data->'location'->'area'->>0) = (raw_data->'location'->'area'->>-1) THEN NULL
                    ELSE (raw_data->'location'->'area'->>-1)::TEXT
                END


            WHEN source = 'jooble' THEN
                CASE
                    -- Jooble countains city name inside the snippet
                    WHEN (raw_data->>'snippet') ~* '\y(wien|vienna|graz|linz|salzburg|innsbruck|berlin|munich|münchen|hamburg|frankfurt)\y'
                    THEN (regexp_match(LOWER(raw_data->>'snippet'), '\y(wien|vienna|graz|linz|salzburg|innsbruck|berlin|munich|münchen|hamburg|frankfurt)\y'))[1]
                    ELSE NULL
                END

            WHEN source = 'arbeitnow' THEN
                CASE
                    -- Arbeitnow gives the city name usually as first or only string
                    WHEN TRIM(raw_data->>'location') ILIKE 'Austria' OR TRIM(raw_data->>'location') ILIKE 'Germany' THEN NULL
                    ELSE TRIM(split_part((raw_data->>'location')::TEXT, ',', 1))
                END

            ELSE NULL
        END AS job_city,

        CASE
            WHEN source = 'adzuna' THEN (raw_data->>'redirect_url')::TEXT
            WHEN source = 'jooble' THEN (raw_data->>'link')::TEXT
            ELSE (raw_data->>'url')::TEXT
        END AS redirect_url,

        CASE
            WHEN source = 'adzuna' THEN (raw_data->>'created')::TIMESTAMPTZ
            WHEN source = 'jooble' THEN (raw_data->>'updated')::TIMESTAMPTZ

            WHEN source = 'arbeitnow' THEN to_timestamp((raw_data->>'created_at')::DOUBLE PRECISION)
            ELSE (raw_data->>'created_at')::TIMESTAMPTZ
        END AS job_posted_at,

        regexp_match(
            LOWER(TRIM(CONCAT(raw_data->>'description', ' ', raw_data->>'snippet', ' ', raw_data->>'salary'))),
            '(eur|€|\$|usd|£)\s*([0-9]+(?:[.,][0-9]+)?)(k)?\s*(?:[-–]\s*([0-9]+(?:[.,][0-9]+)?)(k)?)?'
        ) AS currency_as_prefix,

        regexp_match(
            LOWER(TRIM(CONCAT(raw_data->>'description', ' ', raw_data->>'snippet', ' ', raw_data->>'salary'))),
            '([0-9]+(?:[.,][0-9]+)?)(k)?\s*(?:[-–]\s*([0-9]+(?:[.,][0-9]+)?)(k)?)?\s*(eur|€|brutto|\$|usd|£)'
        ) AS currency_as_suffix,

        CASE
            WHEN LOWER(TRIM(CONCAT(raw_data->>'description', ' ', raw_data->>'snippet', ' ', raw_data->>'salary'))) ~* '(monatlich|monthly|per month|/monat)' THEN 'monthly'
            WHEN LOWER(TRIM(CONCAT(raw_data->>'description', ' ', raw_data->>'snippet', ' ', raw_data->>'salary'))) ~* '(stündlich|hourly|per hour|/stunde|/hr)' THEN 'hourly'
            ELSE 'yearly'
        END AS salary_period

    FROM {{ source('bronze', 'raw_jobs') }}
),

parsed_numbers AS (
    SELECT
        *,
        -- Extract the currency
        CASE
            WHEN currency_as_prefix IS NOT NULL THEN currency_as_prefix[1]  -- Currency is at the start (Slot 1)
            WHEN currency_as_suffix IS NOT NULL THEN currency_as_suffix[5]  -- Currency is at the end (Slot 5)
            ELSE 'eur'
        END AS raw_currency,

        -- Minimum salary
        CASE
            WHEN currency_as_prefix IS NOT NULL AND currency_as_prefix[2] IS NOT NULL THEN
                CASE
                    -- (65.5k -> 65500)
                    WHEN currency_as_prefix[3] = 'k' THEN
                        REPLACE(currency_as_prefix[2], ',', '.')::NUMERIC * 1000
                    -- (65.000 -> 65000)
                    ELSE
                        REPLACE(REPLACE(currency_as_prefix[2], '.', ''), ',', '')::NUMERIC
                END

            WHEN currency_as_suffix IS NOT NULL AND currency_as_suffix[1] IS NOT NULL THEN
                CASE
                    WHEN currency_as_suffix[2] = 'k' THEN
                        REPLACE(currency_as_suffix[1], ',', '.')::NUMERIC * 1000
                    ELSE
                        REPLACE(REPLACE(currency_as_suffix[1], '.', ''), ',', '')::NUMERIC
                END
        END AS regex_min,


          -- Maximum salary
        CASE
            WHEN currency_as_prefix IS NOT NULL AND currency_as_prefix[4] IS NOT NULL THEN
                CASE

                    WHEN currency_as_prefix[5] = 'k' THEN
                        REPLACE(currency_as_prefix[4], ',', '.')::NUMERIC * 1000

                    ELSE
                        REPLACE(REPLACE(currency_as_prefix[4], '.', ''), ',', '')::NUMERIC
                END

            WHEN currency_as_suffix IS NOT NULL AND currency_as_suffix[3] IS NOT NULL THEN
                CASE
                    WHEN currency_as_suffix[4] = 'k' THEN
                        REPLACE(currency_as_suffix[3], ',', '.')::NUMERIC * 1000
                    ELSE
                        REPLACE(REPLACE(currency_as_suffix[3], '.', ''), ',', '')::NUMERIC
                END
        END AS regex_max

    FROM extracted_jobs_base
),

enriched_attributes AS (
    SELECT
        job_id,
        source,
        keyword,
        extracted_at,
        job_title,
        company_name,
        job_description,
        job_country,
        job_city,
        redirect_url,
        job_posted_at,
        salary_period,
        raw_currency,

        COALESCE((raw_data->>'salary_min')::NUMERIC, regex_min) AS base_salary_min,
        COALESCE((raw_data->>'salary_max')::NUMERIC, regex_max) AS base_salary_max,

         -- Employment type detectiom
        CASE

            WHEN source = 'adzuna' AND (raw_data->>'contract_time') ILIKE '%full_time%' THEN 'vollzeit'
            WHEN source = 'adzuna' AND (raw_data->>'contract_time') ILIKE '%part_time%' THEN 'teilzeit'

            WHEN source = 'jooble' AND (raw_data->>'type') ILIKE '%Full-time%' THEN 'vollzeit'
            WHEN source = 'jooble' AND (raw_data->>'type') ILIKE '%Part-time%' THEN 'teilzeit'

            WHEN job_description ~* 'full/\s*part[- ]time:\s*full[- ]time' THEN 'vollzeit'
            WHEN job_description ~* 'full/\s*part[- ]time:\s*part[- ]time' THEN 'teilzeit'


            WHEN job_description ~* '\y(vollzeit|full[- ]time|100%)\y' THEN 'vollzeit'
            WHEN job_description ~* '\y(teilzeit|part[- ]time)\y' THEN 'teilzeit'
            WHEN job_description ~* '\y(minijob|geringfügig)\y' THEN 'minijob'

            ELSE 'not_specified'
        END AS employment_type,

        -- Remote
        CASE
            WHEN source = 'arbeitnow' AND (raw_data->>'remote')::BOOLEAN = true THEN 'yes'
            WHEN source = 'arbeitnow' AND (raw_data->>'remote')::BOOLEAN = false THEN 'no'
            WHEN job_description ~* '\y(remote|home[- ]?office|work from home|wfh)\y' THEN 'yes'
            WHEN job_description ~* '\y(vor ort|on[- ]?site|präsenz|office[- ]based)\y' THEN 'no'
            ELSE 'not_specified'
        END AS is_remote,

     -- Position level
        CASE

            WHEN LOWER(job_title) ~* '\y(senior|sr\.?|lead|principal|head|manager)\y' THEN 'senior'
            WHEN LOWER(job_title) ~* '\y(junior|jr\.?|berufseinsteiger|entry[- ]level)\y' THEN 'junior'
            WHEN LOWER(job_title) ~* '\y(werkstudent|working student)\y' THEN 'werkstudent'
            WHEN LOWER(job_title) ~* '\y(praktikum|intern|praktikant|internship)\y' THEN 'internship'
            WHEN LOWER(job_title) ~* '\y(trainee)\y' THEN 'trainee'
            WHEN LOWER(job_title) ~* '\y(medior|mid[- ]level|regular)\y' THEN 'middle'


            WHEN source = 'arbeitnow' AND (raw_data->>'job_types') ~* '\y(senior|lead|principal|head|manager)\y' THEN 'senior'
            WHEN source = 'arbeitnow' AND (raw_data->>'job_types') ~* '\y(berufserfahren|professional|experienced)\y' THEN 'middle'
            WHEN source = 'arbeitnow' AND (raw_data->>'job_types') ~* '\y(junior|berufseinsteiger|entry[- ]level)\y' THEN 'junior'

            ELSE 'not_specified'
        END AS position_level,

        -- Language
        CASE

            WHEN job_description ~* '\y(english only|working language.{0,10}english|no german required)\y' THEN 'english_only'

            WHEN job_description ~* '\y(muttersprache|verhandlungssicher|c1|c2|fließend deutsch|german.{0,15}required)\y' THEN 'german_fluent'

            WHEN job_description ~* '\y(b1|b2|grundkenntnisse|german.{0,15}plus|german.{0,15}advantage|nice to have)\y' THEN 'german_basic'

            ELSE 'not_specified'
        END AS language_requirement

    FROM parsed_numbers
),

normalized_salary AS (
    SELECT
        *,

        (base_salary_min IS NOT NULL AND base_salary_max IS NOT NULL) AS salary_is_range,


        CASE
            WHEN raw_currency IN ('€', 'eur', 'brutto') THEN 'EUR'
            WHEN raw_currency IN ('$', 'usd') THEN 'USD'
            WHEN raw_currency IN ('£', 'gbp') THEN 'GBP'
            ELSE 'EUR'
        END AS normalized_currency,



        CASE
            WHEN salary_period = 'monthly' THEN base_salary_min * 14
            WHEN salary_period = 'hourly' THEN base_salary_min * 1800
            ELSE base_salary_min
        END AS salary_min_yearly,

        CASE
            WHEN salary_period = 'monthly' THEN base_salary_max * 14
            WHEN salary_period = 'hourly' THEN base_salary_max * 1800
            ELSE base_salary_max
        END AS salary_max_yearly

    FROM enriched_attributes

),

quality_scored_cte AS (
    SELECT
        job_id,
        source,
        keyword,
        extracted_at,
        job_title,
        company_name,
        job_description,
        job_country,
        job_city,
        redirect_url,
        job_posted_at,
        salary_is_range,
        salary_period,
        normalized_currency,
        employment_type,
        is_remote,
        position_level,
        language_requirement,


        CASE WHEN salary_min_yearly < 10000 THEN NULL ELSE salary_min_yearly END AS salary_min_yearly,
        CASE WHEN salary_max_yearly < 10000 THEN NULL ELSE salary_max_yearly END AS salary_max_yearly,

        CASE
            WHEN job_description IS NULL THEN 'Tier 3 - Missing Description'
            WHEN LENGTH(job_description) < 150 THEN 'Tier 3 - Too Short'
            WHEN job_description ILIKE '%India%'
                OR job_description ILIKE '%Bangalore%'
                OR job_description ILIKE '%Hyderabad%'
                OR job_country IN ('IN','US')
            THEN 'Tier 3 - Geo Mismatch'
            WHEN LENGTH(job_description) < 500 THEN 'Tier 2 - Partial'
            ELSE 'Tier 1 - Valid'
        END AS description_quality_tier

    FROM normalized_salary
),

deduplicated AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY job_id, source
            ORDER BY extracted_at DESC
        ) AS row_num
    FROM quality_scored_cte
)

SELECT
    job_id,
    source,
    keyword,
    extracted_at as extracted_at_utc,
    job_title,
    company_name,
    job_description,
    job_country,
    job_city,
    redirect_url,
    job_posted_at as job_posted_at_utc,
    salary_is_range,
    salary_min_yearly,
    salary_max_yearly,
    normalized_currency,
    employment_type,
    is_remote,
    position_level,
    language_requirement,
    description_quality_tier
FROM deduplicated
WHERE row_num = 1
