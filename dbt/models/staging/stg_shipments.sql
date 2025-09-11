{{ config(
    materialized='incremental',
    unique_key=['shipment_id', 'valid_from'],
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
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY shipment_id ORDER BY ingestion_ts DESC) AS rn
        FROM enriched
    ) sub
    WHERE rn = 1
),

fk_checked AS (
    SELECT d.*,
           h.order_id AS valid_order_id
    FROM deduped d
    LEFT JOIN {{ ref('stg_orders_header') }} h
        ON d.order_id = h.order_id
)

{% if is_incremental() %}
    -- Insert new or changed valid shipments
    SELECT
        new_data.shipment_id,
        new_data.order_id,
        new_data.carrier,
        new_data.shipped_at_utc,
        new_data.delivered_at_utc,
        new_data.ship_cost,
        new_data.ingestion_ts,
        new_data.src_filename,
        new_data.src_row_hash,
        new_data.delivery_days,
        new_data.on_time_flag,
        new_data.ingestion_ts AS valid_from,
        NULL AS valid_to,
        TRUE AS is_current
    FROM fk_checked new_data
    WHERE valid_order_id IS NOT NULL

    UNION ALL

    -- Expire old versions where data changed
    SELECT
        old_data.shipment_id,
        old_data.order_id,
        old_data.carrier,
        old_data.shipped_at_utc,
        old_data.delivered_at_utc,
        old_data.ship_cost,
        old_data.ingestion_ts,
        old_data.src_filename,
        old_data.src_row_hash,
        old_data.delivery_days,
        old_data.on_time_flag,
        old_data.valid_from,
        new_data.ingestion_ts AS valid_to,
        FALSE AS is_current
    FROM {{ this }} old_data
    INNER JOIN fk_checked new_data
        ON old_data.shipment_id = new_data.shipment_id
    WHERE old_data.is_current = TRUE
      AND old_data.src_row_hash <> new_data.src_row_hash
      AND new_data.valid_order_id IS NOT NULL
{% else %}
    -- Initial load
    SELECT
        shipment_id,
        order_id,
        carrier,
        shipped_at_utc,
        delivered_at_utc,
        ship_cost,
        ingestion_ts,
        src_filename,
        src_row_hash,
        delivery_days,
        on_time_flag,
        ingestion_ts AS valid_from,
        NULL AS valid_to,
        TRUE AS is_current
    FROM fk_checked
    WHERE valid_order_id IS NOT NULL
{% endif %}
