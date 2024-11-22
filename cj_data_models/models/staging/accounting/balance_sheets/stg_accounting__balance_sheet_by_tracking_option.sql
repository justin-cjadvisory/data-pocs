{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:balance_sheets"]
  )
}}

WITH json_base AS (
    SELECT *
    FROM  {{ ref('base_accounting__balance_sheet_by_tracking_option') }}
),

unflatten_and_cast AS (
    SELECT
        CAST(value.Amount AS FLOAT64) AS amount,
        CAST(value.AccountLineType AS STRING) AS account_line_type,
        CAST(value.DataFileID AS STRING) AS data_file_id,
        CAST(value.DataFileCode AS STRING) AS data_file_code,
        CAST(value.StandardLayout AS BOOLEAN) AS standard_layout,
        CAST(value.AccountID AS STRING) AS account_id,
        CAST(value.LineType AS STRING) AS line_type,
        CAST(value.TrackingOptionID2 AS STRING) AS tracking_option_id2,
        CAST(value.TrackingOptionID AS STRING) AS tracking_option_id,
        CAST(value.AccountName AS STRING) AS account_name,
        CAST(value.Date AS DATE) AS date,
        CAST(value.DataFileName AS STRING) AS data_file_name,
        CAST(value.PaymentsOnly AS BOOLEAN) AS payments_only
    FROM json_base,
    UNNEST(value) AS value
)

SELECT * FROM unflatten_and_cast
