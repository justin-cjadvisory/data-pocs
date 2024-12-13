{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:profit_and_loss"]
  )
}}

WITH json_base AS (
    SELECT *
    FROM  {{ ref('base_accounting__profit_and_loss_advanced') }}
),

unflatten_and_cast AS (
    SELECT
        CAST(amount.Amount AS FLOAT64) AS amount,
        CAST(line.AccountName AS STRING) AS account_name,
        CAST(line.AccountID AS STRING) AS account_id,
        CAST(line.AccountLineType AS STRING) AS account_line_type,
        CAST(line.LineType AS STRING) AS line_type,
        CAST(val.UpdatedDateUTC AS TIMESTAMP) AS updated_date_utc,
        CAST(val.PaymentsOnly AS BOOLEAN) AS payments_only,
        CAST(val.StandardLayout AS BOOLEAN) AS standard_layout,
        CAST(val.ToDate AS DATE) AS to_date,
        CAST(val.FromDate AS DATE) AS from_date,
        CAST(val.TimeFrame AS STRING) AS time_frame,
        CAST(val.TrackingCategoryID AS STRING) AS tracking_category_id,
        CAST(val.TrackingCategoryID2 AS STRING) AS tracking_category_id2,
        CAST(val.TrackingOptionID AS STRING) AS tracking_option_id,
        CAST(val.TrackingOptionID2 AS STRING) AS tracking_option_id2,
        CAST(val.Periods AS STRING) AS periods,
        CAST(val.DataFile.DataFileCode AS STRING) AS datafile_code,
        CAST(val.DataFile.DataFileName AS STRING) AS datafile_name,
        CAST(val.DataFile.DataFileID AS STRING) AS datafile_id
    FROM json_base,
    UNNEST(value) AS val,
    UNNEST(val.Lines) AS line,
    UNNEST(line.Amounts) AS amount
    QUALIFY ROW_NUMBER() OVER (PARTITION BY updated_date_utc, account_id ORDER BY updated_date_utc DESC) = 1
)

SELECT * FROM unflatten_and_cast
WHERE account_line_type != 'Total'