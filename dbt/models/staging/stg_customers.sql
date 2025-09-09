{{ config(
    materialized='incremental',
    unique_key='customer_id',
    on_schema_change='append_new_columns'
) }}

WITH src AS (
    SELECT *
    FROM {{ source('bronze', 'customers') }}
    {% if is_incremental() %}
        WHERE {{ normalize_timestamp('ingestion_ts') }} > (
            SELECT MAX(ingestion_ts) FROM {{ this }}
        )
    {% endif %}
),

typed AS (
    SELECT
        CAST(customer_id AS bigint) AS customer_id,
        natural_key,
        TRIM(first_name) AS first_name,
        TRIM(last_name) AS last_name,
        LOWER(TRIM(email)) AS email,
        TRIM(phone) AS phone,
        TRIM(address_line1) AS address_line1,
        TRIM(address_line2) AS address_line2,
        TRIM(city) AS city,
        TRIM(state_region) AS state_region,
        TRIM(postcode) AS postcode,
        UPPER(TRIM(country_code)) AS country_code,
        CAST(latitude AS double) AS latitude,
        CAST(longitude AS double) AS longitude,
        {{normalize_timestamp('birth_date')}} AS birth_date_utc,
        {{ normalize_timestamp('join_ts') }} AS join_ts_utc,
        CAST(is_vip AS boolean) AS is_vip,
        CAST(gdpr_consent AS boolean) AS gdpr_consent,
        {{ normalize_timestamp('ingestion_ts') }} AS ingestion_ts,
        src_filename,
        src_row_hash
    FROM src
),

enriched AS (
    SELECT *,
           CONCAT(first_name, ' ', last_name) AS full_name,
           DATE_DIFF('year', birth_date_utc, CURRENT_DATE) AS age
    FROM typed
),

deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY natural_key ORDER BY ingestion_ts DESC) AS rn
    FROM enriched
    QUALIFY rn = 1
)

SELECT * EXCLUDE (rn) FROM deduped
