
{{ config(
    materialized='incremental',
    unique_key=['sensor_ts_utc', 'store_id', 'shelf_id'],
    on_schema_change='append_new_columns'
) }}

WITH src AS (
    SELECT *
    FROM {{ source('bronze', 'sensors') }}
    {% if is_incremental() %}
        WHERE {{ normalize_timestamp('ingestion_ts') }} > (
            SELECT MAX(ingestion_ts) FROM {{ this }}
        )
    {% endif %}
),

typed AS (
    SELECT
        {{ normalize_timestamp('sensor_ts') }} AS sensor_ts_utc,
        CAST(store_id AS bigint) AS store_id,
        TRIM(shelf_id) AS shelf_id,
        CAST(temperature_c AS DECIMAL(5,2)) AS temperature_c,
        CAST(humidity_pct AS DECIMAL(5,2)) AS humidity_pct,
        CAST(battery_mv AS int) AS battery_mv,
        TRIM(month) AS month,
        {{ normalize_timestamp('ingestion_ts') }} AS ingestion_ts,
        src_filename,
        src_row_hash
    FROM src
),

deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY sensor_ts_utc, store_id, shelf_id, month
               ORDER BY ingestion_ts DESC
           ) AS rn
    FROM typed
    QUALIFY rn = 1
)

SELECT * EXCLUDE (rn) FROM deduped
