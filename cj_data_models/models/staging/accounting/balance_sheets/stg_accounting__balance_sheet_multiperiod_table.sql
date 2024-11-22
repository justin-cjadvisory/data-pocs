{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:balance_sheets"]
  )
}}

WITH json_base AS (
    SELECT *
    FROM {{ ref('base_accounting__balance_sheet_multiperiod_table') }}
),

unflatten_and_cast AS (
    SELECT
        CAST(value.Amount AS FLOAT64) AS amount,
        CAST(value.AccountLineType AS STRING) AS account_line_type,
        CAST(value.DataFileCode AS STRING) AS data_file_code,
        CAST(value.DataFileID AS STRING) AS data_file_id,
        CAST(value.StandardLayout AS BOOLEAN) AS standard_layout,
        CAST(value.AccountID AS STRING) AS account_id,
        CAST(value.LineType AS STRING) AS line_type,
        CAST(value.PaymentsOnly AS BOOLEAN) AS payments_only,
        CAST(value.Period AS STRING) AS period,
        CAST(value.AccountName AS STRING) AS account_name,
        CAST(value.Date AS DATE) AS date,
        CAST(value.DataFileName AS STRING) AS data_file_name
    FROM json_base,
    UNNEST(value) AS value
)

SELECT * FROM unflatten_and_cast
