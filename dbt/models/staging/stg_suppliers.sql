
{{ config(
    materialized='table',
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
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY ingestion_ts DESC) AS rn
    FROM enriched
    QUALIFY rn = 1
)

SELECT * EXCLUDE (rn) FROM deduped
