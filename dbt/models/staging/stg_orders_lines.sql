{{ config(
    materialized='incremental',
    unique_key=['order_id','line_number'],
    on_schema_change='append_new_columns'
) }}

WITH src AS (
    SELECT *
    FROM {{ source('bronze', 'orders_lines') }}
    {% if is_incremental() %}
        WHERE {{ normalize_timestamp('ingestion_ts') }} > (
            SELECT MAX(ingestion_ts) FROM {{ this }}
        )
    {% endif %}
),

typed AS (
    SELECT
        CAST(order_id AS bigint) AS order_id,
        CAST(line_number AS int) AS line_number,
        CAST(product_id AS bigint) AS product_id,
        CAST(qty AS int) AS qty,
        CAST(unit_price AS DECIMAL(12,4)) AS unit_price,
        CAST(line_discount_pct AS DECIMAL(5,4)) AS line_discount_pct,
        CAST(tax_pct AS DECIMAL(5,4)) AS tax_pct,
        CAST(order_dt AS DATE) AS order_dt,  
        {{ normalize_timestamp('ingestion_ts') }} AS ingestion_ts,
        src_filename,
        src_row_hash
    FROM src
),

enriched AS (
    SELECT
        *,
        (qty * unit_price) AS line_total,
        (qty * unit_price) * line_discount_pct AS discount_amount,
        ((qty * unit_price) - ((qty * unit_price) * line_discount_pct)) * tax_pct AS tax_amount
    FROM typed
),

deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY order_id, line_number ORDER BY ingestion_ts DESC) AS rn
    FROM enriched
    QUALIFY rn = 1
)

SELECT * EXCLUDE (rn) FROM deduped