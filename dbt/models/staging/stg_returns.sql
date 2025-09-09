{{ config(
    materialized='incremental',
    unique_key='return_id',
    on_schema_change='append_new_columns'
) }}

WITH src AS (
    SELECT *
    FROM {{ source('bronze', 'returns') }}
    {% if is_incremental() %}
        WHERE {{ normalize_timestamp('ingestion_ts') }} > (
            SELECT MAX(ingestion_ts) FROM {{ this }}
        )
    {% endif %}
),

typed AS (
    SELECT
        CAST(return_id AS bigint) AS return_id,
        CAST(order_id AS bigint) AS order_id,
        CAST(product_id AS bigint) AS product_id,
        {{ normalize_timestamp('return_ts') }} AS return_ts_utc,
        CAST(qty AS int) AS qty,
        TRIM(reason) AS reason,
        {{ normalize_timestamp('ingestion_ts') }} AS ingestion_ts,
        src_filename,
        src_row_hash
    FROM src
),

deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY return_id ORDER BY ingestion_ts DESC) AS rn
    FROM typed
    QUALIFY rn = 1
)

SELECT * EXCLUDE (rn) FROM deduped