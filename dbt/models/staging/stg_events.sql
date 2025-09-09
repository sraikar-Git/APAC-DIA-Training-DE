{{ config(
    materialized='incremental',
    unique_key='event_id',
    on_schema_change='append_new_columns'
) }}

WITH src AS (
    SELECT *
    FROM {{ source('bronze', 'events') }}
    {% if is_incremental() %}
        WHERE {{ normalize_timestamp('ingestion_ts') }} > (
            SELECT MAX(ingestion_ts) FROM {{ this }})
    {% endif %}
),

-- Step 1: Extract JSON fields from nested structure
json_parsed AS (
    SELECT
        -- envelope fields
        json_extract_string(json, '$.envelope.event_id') AS event_id,  -- keep as string
        json_extract_string(json, '$.envelope.event_type') AS event_type,
        CAST(json_extract_string(json, '$.envelope.user_id') AS BIGINT) AS user_id,
        json_extract_string(json, '$.envelope.session_id') AS session_id,
        json_extract_string(json, '$.envelope.event_ts') AS raw_event_ts,

        -- payload fields
        CAST(json_extract_string(json, '$.payload.product_id') AS BIGINT) AS product_id,
        json_extract_string(json, '$.payload.page') AS page,
        json_extract_string(json, '$.payload.referrer') AS referrer,
        json_extract_string(json, '$.payload.device') AS device,
        json_extract_string(json, '$.payload.metadata.browser') AS browser,
        json_extract_string(json, '$.payload.metadata.version') AS version,

        -- bronze columns
        CAST(event_date AS DATE) AS event_date,
        ingestion_ts,
        src_filename,
        src_row_hash
    FROM src
),

-- Step 2: Apply timestamp normalization
typed AS (
    SELECT
        event_id,
        event_type,
        user_id,
        session_id,
        product_id,
        page,
        referrer,
        device,
        browser,
        version,
        {{ normalize_timestamp('raw_event_ts') }} AS event_ts_utc,
        event_date,
        {{ normalize_timestamp('ingestion_ts') }} AS ingestion_ts,
        src_filename,
        src_row_hash
    FROM json_parsed
),

-- Step 3: Deduplicate by latest ingestion
deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingestion_ts DESC) AS rn
    FROM typed
    QUALIFY rn = 1
)

SELECT * EXCLUDE (rn) FROM deduped
