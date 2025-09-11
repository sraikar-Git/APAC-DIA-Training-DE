{{ config(
    materialized='incremental',
    unique_key=['customer_id', 'valid_from'],
    on_schema_change='append_new_columns'
) }}

WITH src AS (
    SELECT *
    FROM {{ source('bronze', 'customers') }}
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
        {{ normalize_timestamp('birth_date') }} AS birth_date_utc,
        {{ normalize_timestamp('join_ts') }} AS join_ts_utc,
        CAST(is_vip AS boolean) AS is_vip,
        CAST(gdpr_consent AS boolean) AS gdpr_consent,
        {{ normalize_timestamp('ingestion_ts') }} AS ingestion_ts,
        src_filename,
        src_row_hash
    FROM src
),

deduped AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY natural_key ORDER BY ingestion_ts DESC) AS rn
        FROM typed
    ) sub
    WHERE rn = 1
)

{% if is_incremental() %}
    -- 1️⃣ New or changed rows
    SELECT
        new_data.customer_id,
        new_data.natural_key,
        new_data.first_name,
        new_data.last_name,
        new_data.email,
        new_data.phone,
        new_data.address_line1,
        new_data.address_line2,
        new_data.city,
        new_data.state_region,
        new_data.postcode,
        new_data.country_code,
        new_data.latitude,
        new_data.longitude,
        new_data.birth_date_utc,
        new_data.join_ts_utc,
        new_data.is_vip,
        new_data.gdpr_consent,
        new_data.ingestion_ts,
        new_data.src_filename,
        new_data.src_row_hash,
        new_data.ingestion_ts AS valid_from,
        NULL AS valid_to,
        TRUE AS is_current
    FROM deduped new_data

    UNION ALL

    -- 2️⃣ Expire old versions
    SELECT
        old_data.customer_id,
        old_data.natural_key,
        old_data.first_name,
        old_data.last_name,
        old_data.email,
        old_data.phone,
        old_data.address_line1,
        old_data.address_line2,
        old_data.city,
        old_data.state_region,
        old_data.postcode,
        old_data.country_code,
        old_data.latitude,
        old_data.longitude,
        old_data.birth_date_utc,
        old_data.join_ts_utc,
        old_data.is_vip,
        old_data.gdpr_consent,
        old_data.ingestion_ts,
        old_data.src_filename,
        old_data.src_row_hash,
        old_data.valid_from,
        new_data.ingestion_ts AS valid_to,
        FALSE AS is_current
    FROM {{ this }} old_data
    INNER JOIN deduped new_data
        ON old_data.customer_id = new_data.customer_id
    WHERE old_data.is_current = TRUE
      AND old_data.src_row_hash <> new_data.src_row_hash
{% else %}
    -- Initial load
    SELECT
        customer_id,
        natural_key,
        first_name,
        last_name,
        email,
        phone,
        address_line1,
        address_line2,
        city,
        state_region,
        postcode,
        country_code,
        latitude,
        longitude,
        birth_date_utc,
        join_ts_utc,
        is_vip,
        gdpr_consent,
        ingestion_ts,
        src_filename,
        src_row_hash,
        ingestion_ts AS valid_from,
        NULL AS valid_to,
        TRUE AS is_current
    FROM deduped
{% endif %}
