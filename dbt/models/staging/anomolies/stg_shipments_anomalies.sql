{{ config(
    materialized='incremental',
    unique_key='shipment_id',
    on_schema_change='append_new_columns'
) }}

WITH src AS (
    SELECT *
    FROM {{ source('bronze', 'shipments') }}
),

typed AS (
    SELECT
        CAST(shipment_id AS bigint) AS shipment_id,
        CAST(order_id AS bigint) AS order_id,
        TRIM(carrier) AS carrier,
        {{ normalize_timestamp('shipped_at') }} AS shipped_at_utc,
        {{ normalize_timestamp('delivered_at') }} AS delivered_at_utc,
        CAST(ship_cost AS DECIMAL(12,2)) AS ship_cost,
        {{ normalize_timestamp('ingestion_ts') }} AS ingestion_ts,
        src_filename,
        src_row_hash
    FROM src
),

enriched AS (
    SELECT
        *,
        DATE_DIFF('day', shipped_at_utc, delivered_at_utc) AS delivery_days,
        CASE
            WHEN delivered_at_utc IS NOT NULL 
                 AND DATE_DIFF('day', shipped_at_utc, delivered_at_utc) <= 3 THEN TRUE
            ELSE FALSE
        END AS on_time_flag
    FROM typed
),

deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY shipment_id ORDER BY ingestion_ts DESC) AS rn
    FROM enriched
    QUALIFY rn = 1
),

fk_checked AS (
    SELECT d.*,
           h.order_id AS valid_order_id
    FROM deduped d
    LEFT JOIN {{ ref('stg_orders_header') }} h
        ON d.order_id = h.order_id
)

-- 🚨 Keep only invalid order_id rows
SELECT
    *,
    CASE
        WHEN valid_order_id IS NULL THEN 'Invalid order_id'
    END AS anomaly_reason
FROM fk_checked
WHERE valid_order_id IS NULL
