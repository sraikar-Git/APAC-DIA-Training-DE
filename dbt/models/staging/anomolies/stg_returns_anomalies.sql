{{ config(
    materialized='incremental',
    unique_key='return_id',
    on_schema_change='append_new_columns'
) }}

WITH src AS (
    SELECT *
    FROM {{ source('bronze', 'returns') }}
    {% if is_incremental() %}
        WHERE (
            {{ normalize_timestamp('ingestion_ts') }} >= (
                SELECT COALESCE(MIN(ingestion_ts), '1900-01-01')
                FROM {{ this }}
            )
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
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY return_id ORDER BY ingestion_ts DESC) AS rn
        FROM typed
    ) sub
    WHERE rn = 1
),

fk_checked AS (
    SELECT d.*,
           o.order_id AS valid_order_id
    FROM deduped d
    LEFT JOIN {{ ref('stg_orders_header') }} o
        ON d.order_id = o.order_id
)

-- 🚨 Final anomaly output: only invalid FK rows
SELECT 
    * EXCLUDE (valid_order_id),
    CASE
        WHEN valid_order_id IS NULL THEN 'Invalid order_id'
    END AS anomaly_reason
FROM fk_checked
WHERE valid_order_id IS NULL
