{{ config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='append_new_columns'
) }}

WITH src AS (
    SELECT *
    FROM {{ source('bronze', 'orders_header') }}
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
        CAST(order_id AS bigint) AS order_id,
        {{ normalize_timestamp('order_ts') }} AS order_ts_utc,
        CAST(order_dt_local AS date) AS order_dt_local,
        CAST(customer_id AS bigint) AS customer_id,
        CAST(store_id AS bigint) AS store_id,
        LOWER(TRIM(channel)) AS channel,
        LOWER(TRIM(payment_method)) AS payment_method,
        TRIM(coupon_code) AS coupon_code,
        CAST(shipping_fee AS DECIMAL(12,2)) AS shipping_fee,
        UPPER(TRIM(currency)) AS currency,
        {{ normalize_timestamp('ingestion_ts') }} AS ingestion_ts,
        src_filename,
        src_row_hash
    FROM src
),

deduped AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY ingestion_ts DESC) AS rn
        FROM typed
    ) sub
    WHERE rn = 1
),

fk_checked AS (
    SELECT d.*,
           c.customer_id AS valid_customer_id,
           s.store_id AS valid_store_id
    FROM deduped d
    LEFT JOIN {{ ref('stg_customers') }} c
        ON d.customer_id = c.customer_id
    LEFT JOIN {{ ref('stg_stores') }} s
        ON d.store_id = s.store_id
)

-- 🚨 Final anomaly output: only invalid FK rows
SELECT 
    * EXCLUDE (valid_customer_id, valid_store_id),
    CASE
        WHEN valid_customer_id IS NULL AND valid_store_id IS NULL THEN 'Invalid customer_id and store_id'
        WHEN valid_customer_id IS NULL THEN 'Invalid customer_id'
        WHEN valid_store_id IS NULL THEN 'Invalid store_id'
    END AS anomaly_reason
FROM fk_checked
WHERE valid_customer_id IS NULL
   OR valid_store_id IS NULL
