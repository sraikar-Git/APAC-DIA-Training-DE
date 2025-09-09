{{ config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='append_new_columns'
) }}

WITH src AS (
    SELECT *
    FROM {{ source('bronze', 'orders_header') }}
    {% if is_incremental() %}
        WHERE {{ normalize_timestamp('ingestion_ts') }} > (
            SELECT MAX(ingestion_ts) FROM {{ this }}
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
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY ingestion_ts DESC) AS rn
    FROM typed
    QUALIFY rn = 1
)

SELECT * EXCLUDE (rn) FROM deduped
