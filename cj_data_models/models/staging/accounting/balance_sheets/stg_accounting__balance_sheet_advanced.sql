{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:balance_sheets"]
  )
}}

WITH json_base AS (
    SELECT *
    FROM  {{ ref('base_accounting__balance_sheet_advanced') }}
),

unflatten_and_cast AS (
    SELECT
        CAST(val.Amount AS FLOAT64) AS amount,
        CAST(val.LineDescription AS STRING) AS line_description,
        CAST(val.AccountLineType AS STRING) AS account_line_type,
        CAST(val.AccountName AS STRING) AS account_name,
        CAST(val.LineType AS STRING) AS line_type,
        CAST(val.TrackingOptionID2 AS STRING) AS tracking_option_id2,
        CAST(val.TrackingOptionID AS STRING) AS tracking_option_id,
        CAST(val.AccountID AS STRING) AS account_id,
        CAST(val.DataFileID AS STRING) AS data_file_id,
        CAST(val.DataFileCode AS STRING) AS data_file_code,
        CAST(val.StandardLayout AS BOOLEAN) AS standard_layout,
        CAST(val.Date AS DATE) AS date,
        CAST(val.TimeFrame AS STRING) AS time_frame,
        CAST(val.DataFileName AS STRING) AS data_file_name,
        CAST(val.Periods AS INT64) AS periods,
        CAST(val.PaymentsOnly AS BOOLEAN) AS payments_only
    FROM json_base, 
    UNNEST(value) AS val
    QUALIFY ROW_NUMBER() OVER (PARTITION BY date, account_id ORDER BY date DESC) = 1
)


SELECT * FROM unflatten_and_cast
