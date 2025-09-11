{{ config(
    materialized='incremental',
    unique_key=['product_id', 'valid_from'],
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
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY ingestion_ts DESC) AS rn
        FROM typed
    ) sub
    WHERE rn = 1
)

{% if is_incremental() %}
    -- 1️⃣ New or changed rows
    SELECT
        new_data.product_id,
        new_data.sku,
        new_data.name,
        new_data.category,
        new_data.subcategory,
        new_data.current_price,
        new_data.currency,
        new_data.is_discontinued,
        new_data.introduced_dt,
        new_data.discontinued_dt,
        new_data.ingestion_ts,
        new_data.src_filename,
        new_data.src_row_hash,
        new_data.ingestion_ts AS valid_from,
        NULL AS valid_to,
        TRUE AS is_current
    FROM deduped new_data

    UNION ALL

    -- 2️⃣ Expire old versions
    SELECT
        old_data.product_id,
        old_data.sku,
        old_data.name,
        old_data.category,
        old_data.subcategory,
        old_data.current_price,
        old_data.currency,
        old_data.is_discontinued,
        old_data.introduced_dt,
        old_data.discontinued_dt,
        old_data.ingestion_ts,
        old_data.src_filename,
        old_data.src_row_hash,
        old_data.valid_from,
        new_data.ingestion_ts AS valid_to,
        FALSE AS is_current
    FROM {{ this }} old_data
    INNER JOIN deduped new_data
        ON old_data.product_id = new_data.product_id
    WHERE old_data.is_current = TRUE
      AND old_data.src_row_hash <> new_data.src_row_hash
{% else %}
    -- Initial load
    SELECT
        product_id,
        sku,
        name,
        category,
        subcategory,
        current_price,
        currency,
        is_discontinued,
        introduced_dt,
        discontinued_dt,
        ingestion_ts,
        src_filename,
        src_row_hash,
        ingestion_ts AS valid_from,
        NULL AS valid_to,
        TRUE AS is_current
    FROM deduped
{% endif %}
