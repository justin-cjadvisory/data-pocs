{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:profit_and_loss"]
  )
}}

WITH json_base AS (
    SELECT *
    FROM  {{ ref('base_accounting__profit_and_loss_by_tracking_category') }}
),

unflatten_and_cast AS (
    SELECT
        CAST(val.TrackingCategoryID2 AS STRING) AS tracking_category_id2,
        CAST(val.UpdatedDateUTC AS TIMESTAMP) AS updated_date_utc,
        CAST(val.PaymentsOnly AS BOOLEAN) AS payments_only,
        CAST(val.StandardLayout AS BOOLEAN) AS standard_layout,
        CAST(val.ToDate AS DATE) AS to_date,
        CAST(val.FromDate AS DATE) AS from_date,
        CAST(val.TrackingCategoryID AS STRING) AS tracking_category_id,
        CAST(amount.Amount AS FLOAT64) AS amount,
        CAST(amount.TrackingOption2.Option AS STRING) AS tracking_option_2,
        CAST(amount.TrackingOption.Option AS STRING) AS tracking_option_1,
        CAST(val.DataFile.DataFileCode AS STRING) AS datafile_code,
        CAST(val.DataFile.DataFileName AS STRING) AS datafile_name,
        CAST(val.DataFile.DataFileID AS STRING) AS datafile_id
    FROM
        json_base,
        UNNEST(value) AS val,               
        UNNEST(val.Lines) AS line,          
        UNNEST(line.Amounts) AS amount      
)

SELECT * FROM unflatten_and_cast
