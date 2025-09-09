{{ config(
    materialized='table',
    on_schema_change='append_new_columns'
) }}

WITH src AS (
    SELECT *
    FROM {{ source('bronze', 'exchange_rates') }}
),

typed AS (
    SELECT
        CAST(date AS date) AS date,
        UPPER(TRIM(currency)) AS currency,
        CAST(rate_to_aud AS DECIMAL(18,8)) AS rate_to_aud,
        {{ normalize_timestamp('ingestion_ts') }} AS ingestion_ts,
        src_filename,
        src_row_hash
    FROM src
),

deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY date, currency ORDER BY ingestion_ts DESC) AS rn
    FROM typed
    QUALIFY rn = 1
)

SELECT * EXCLUDE (rn) FROM deduped
