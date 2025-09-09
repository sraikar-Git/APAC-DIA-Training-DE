{{ config(
    materialized='table',
    on_schema_change='append_new_columns'
) }}

WITH src AS (
    SELECT *
    FROM {{ source('bronze', 'stores') }}
),

typed AS (
    SELECT
        CAST(store_id AS bigint) AS store_id,
        TRIM(store_code) AS store_code,
        TRIM(name) AS name,
        LOWER(TRIM(channel)) AS channel,
        TRIM(region) AS region,
        TRIM(state) AS state,
        CAST(latitude AS double) AS latitude,
        CAST(longitude AS double) AS longitude,
        CAST(open_dt AS date) AS open_dt,
        CAST(close_dt AS date) AS close_dt,
        {{ normalize_timestamp('ingestion_ts') }} AS ingestion_ts,
        src_filename,
        src_row_hash
    FROM src
),

enriched AS (
    SELECT
        *,
        DATE_DIFF('day', open_dt, CURRENT_DATE) AS store_age_days
    FROM typed
),

deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY ingestion_ts DESC) AS rn
    FROM enriched
    QUALIFY rn = 1
)

SELECT * EXCLUDE (rn) FROM deduped
