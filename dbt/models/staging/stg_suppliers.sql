{{ config(
    materialized='incremental',
    unique_key=['supplier_id', 'valid_from'],
    on_schema_change='append_new_columns'
) }}

WITH src AS (
    SELECT *
    FROM {{ source('bronze', 'suppliers') }}
),

typed AS (
    SELECT
        CAST(supplier_id AS bigint) AS supplier_id,
        TRIM(supplier_code) AS supplier_code,
        TRIM(name) AS name,
        UPPER(TRIM(country_code)) AS country_code,
        CAST(lead_time_days AS int) AS lead_time_days,
        CAST(preferred AS boolean) AS preferred,
        {{ normalize_timestamp('ingestion_ts') }} AS ingestion_ts,
        src_filename,
        src_row_hash
    FROM src
),

enriched AS (
    SELECT
        *,
        CASE
            WHEN lead_time_days <= 3 THEN 'Tier 1 - Fast'
            WHEN lead_time_days <= 7 THEN 'Tier 2 - Standard'
            ELSE 'Tier 3 - Slow'
        END AS supplier_tier
    FROM typed
),

deduped AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY ingestion_ts DESC) AS rn
        FROM enriched
    ) sub
    WHERE rn = 1
)

{% if is_incremental() %}
    -- 1️⃣ Insert new or changed supplier versions
    SELECT
        new_data.supplier_id,
        new_data.supplier_code,
        new_data.name,
        new_data.country_code,
        new_data.lead_time_days,
        new_data.preferred,
        new_data.ingestion_ts,
        new_data.src_filename,
        new_data.src_row_hash,
        new_data.supplier_tier,
        new_data.ingestion_ts AS valid_from,
        NULL AS valid_to,
        TRUE AS is_current
    FROM deduped new_data

    UNION ALL

    -- 2️⃣ Expire old versions where data changed
    SELECT
        old_data.supplier_id,
        old_data.supplier_code,
        old_data.name,
        old_data.country_code,
        old_data.lead_time_days,
        old_data.preferred,
        old_data.ingestion_ts,
        old_data.src_filename,
        old_data.src_row_hash,
        old_data.supplier_tier,
        old_data.valid_from,
        new_data.ingestion_ts AS valid_to,
        FALSE AS is_current
    FROM {{ this }} old_data
    INNER JOIN deduped new_data
        ON old_data.supplier_id = new_data.supplier_id
    WHERE old_data.is_current = TRUE
      AND old_data.src_row_hash <> new_data.src_row_hash
{% else %}
    -- Initial full load
    SELECT
        supplier_id,
        supplier_code,
        name,
        country_code,
        lead_time_days,
        preferred,
        ingestion_ts,
        src_filename,
        src_row_hash,
        supplier_tier,
        ingestion_ts AS valid_from,
        NULL AS valid_to,
        TRUE AS is_current
    FROM deduped
{% endif %}
