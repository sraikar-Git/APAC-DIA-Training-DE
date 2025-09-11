{{ config(
    materialized='incremental',
    unique_key=['store_id', 'valid_from'],
    on_schema_change='append_new_columns'
) }}

WITH src AS (
    SELECT *
    FROM {{ source('bronze', 'stores') }}
),

typed AS (
    SELECT
        CAST(store_id AS bigint) AS store_id,
        TRIM(store_code) AS store_code,
        TRIM(name) AS name,
        LOWER(TRIM(channel)) AS channel,
        TRIM(region) AS region,
        TRIM(state) AS state,
        CAST(latitude AS double) AS latitude,
        CAST(longitude AS double) AS longitude,
        CAST(open_dt AS date) AS open_dt,
        CAST(close_dt AS date) AS close_dt,
        {{ normalize_timestamp('ingestion_ts') }} AS ingestion_ts,
        src_filename,
        src_row_hash
    FROM src
),

deduped AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY ingestion_ts DESC) AS rn
        FROM typed
    ) sub
    WHERE rn = 1
)

{% if is_incremental() %}
    -- 1️⃣ New or changed rows
    SELECT
        new_data.store_id,
        new_data.store_code,
        new_data.name,
        new_data.channel,
        new_data.region,
        new_data.state,
        new_data.latitude,
        new_data.longitude,
        new_data.open_dt,
        new_data.close_dt,
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
        old_data.store_id,
        old_data.store_code,
        old_data.name,
        old_data.channel,
        old_data.region,
        old_data.state,
        old_data.latitude,
        old_data.longitude,
        old_data.open_dt,
        old_data.close_dt,
        old_data.ingestion_ts,
        old_data.src_filename,
        old_data.src_row_hash,
        old_data.valid_from,
        new_data.ingestion_ts AS valid_to,
        FALSE AS is_current
    FROM {{ this }} old_data
    INNER JOIN deduped new_data
        ON old_data.store_id = new_data.store_id
    WHERE old_data.is_current = TRUE
      AND old_data.src_row_hash <> new_data.src_row_hash
{% else %}
    -- Initial load
    SELECT
        store_id,
        store_code,
        name,
        channel,
        region,
        state,
        latitude,
        longitude,
        open_dt,
        close_dt,
        ingestion_ts,
        src_filename,
        src_row_hash,
        ingestion_ts AS valid_from,
        NULL AS valid_to,
        TRUE AS is_current
    FROM deduped
{% endif %}
