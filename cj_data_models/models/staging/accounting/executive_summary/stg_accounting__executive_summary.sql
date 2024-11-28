{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:executive_summary"]
  )
}}

WITH json_base AS (
    SELECT *
    FROM {{ ref('base_accounting__executive_summary') }}
),

unflattened_data AS (
    SELECT
        -- Top-level fields from `value`
        CAST(val.UpdatedDateUTC AS TIMESTAMP) AS updated_date_utc,
        CAST(val.Date AS DATE) AS record_date,
        
        -- Fields from `DataFile`
        CAST(val.DataFile.DataFileCode AS STRING) AS datafile_code,
        CAST(val.DataFile.DataFileName AS STRING) AS datafile_name,
        CAST(val.DataFile.DataFileID AS STRING) AS datafile_id,

        -- Fields from `Lines` (unnested)
        CAST(line.Description AS STRING) AS line_description,
        CAST(line.Type AS STRING) AS line_type,
        CAST(line.LastMonthAmount AS FLOAT64) AS line_last_month_amount,
        CAST(line.ThisMonthAmount AS FLOAT64) AS line_this_month_amount,
        CAST(line.VariancePercent AS FLOAT64) AS line_variance_percent

    FROM json_base,
        UNNEST(value) AS val,        -- Unnest `value` array
        UNNEST(val.Lines) AS line    -- Unnest `Lines` array within `value`
)

SELECT *
FROM unflattened_data
