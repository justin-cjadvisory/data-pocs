{{ config(
    materialized='view',
    tags=["module:accounting", "submodule:tracking_categories"]
) }}

WITH base_data AS (
    SELECT *
    FROM {{ ref('base_accounting__tracking_categories') }}  -- Replace with your base model reference
),

unnested_data AS (
    SELECT 
        -- Unnest top-level repeated record
        main.Status AS tracking_status,
        main.Name AS tracking_name,
        main.TrackingCategoryID AS tracking_category_id,

        -- Extract fields from value.DataFile
        main.DataFile.DataFileCode AS data_file_code,
        main.DataFile.DataFileName AS data_file_name,
        main.DataFile.DataFileID AS data_file_id,

        -- Unnest nested repeated record: value.Options
        option.Status AS option_status,
        option.Name AS option_name,
        option.TrackingOptionID AS tracking_option_id
    FROM base_data,
    UNNEST(value) AS main,           -- Unnest `value` field
    UNNEST(main.Options) AS option   -- Unnest `Options` within `value`
)

SELECT *
FROM unnested_data
