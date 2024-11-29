{{ config(
    materialized='view',
    tags=["module:accounting", "submodule:tracking_categories"]
) }}

WITH base_data AS (
    SELECT *
    FROM {{ ref('base_accounting__tracking_categories_table') }}  -- Replace with your base model reference
),

unnested_data AS (
    SELECT 
        -- Extract fields from the repeated record
        main.TrackingCategoryStatus AS tracking_category_status,
        main.TrackingCategoryName AS tracking_category_name,
        main.TrackingCategoryID AS tracking_category_id,
        main.TrackingOptionStatus AS tracking_option_status,
        main.TrackingOptionName AS tracking_option_name,
        main.TrackingOptionID AS tracking_option_id,

        -- Extract fields from the DataFile record
        main.DataFileCode AS data_file_code,
        main.DataFileName AS data_file_name,
        main.DataFileID AS data_file_id
    FROM base_data,
    UNNEST(value) AS main  -- Unnest `value` field
)

SELECT *
FROM unnested_data
