{{ config(
    materialized='table',
    on_schema_change='append_new_columns'
) }}

WITH src AS (
    SELECT *
    FROM {{ source('bronze', 'products') }}
),

typed AS (
    SELECT
        CAST(product_id AS bigint) AS product_id,
        TRIM(sku) AS sku,
        TRIM(name) AS name,
        TRIM(category) AS category,
        TRIM(subcategory) AS subcategory,
        CAST(current_price AS DECIMAL(12,4)) AS current_price,
        UPPER(TRIM(currency)) AS currency,
        CAST(is_discontinued AS boolean) AS is_discontinued,
        CAST(introduced_dt AS date) AS introduced_dt,
        CAST(discontinued_dt AS date) AS discontinued_dt,
        {{ normalize_timestamp('ingestion_ts') }} AS ingestion_ts,
        src_filename,
        src_row_hash
    FROM src
),

deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY ingestion_ts DESC) AS rn
    FROM typed
    QUALIFY rn = 1
)

SELECT * EXCLUDE (rn) FROM deduped