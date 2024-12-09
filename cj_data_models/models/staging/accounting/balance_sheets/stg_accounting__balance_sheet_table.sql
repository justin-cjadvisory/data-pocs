{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:balance_sheets"]
  )
}}

WITH json_base AS (
    SELECT *
    FROM  {{ ref('base_accounting__balance_sheet_table') }}
),

unflatten_and_cast AS (
    SELECT
        CAST(value.Amount AS FLOAT64) AS amount,
        CAST(value.AccountLineType AS STRING) AS account_line_type,
        CAST(value.AccountID AS STRING) AS account_id,
        CAST(value.LineType AS STRING) AS line_type,
        CAST(value.StandardLayout AS BOOLEAN) AS standard_layout,
        CAST(value.AccountName AS STRING) AS account_name,
        CAST(value.Date AS DATE) AS date,
        CAST(value.DataFileName AS STRING) AS data_file_name,
        CAST(value.DataFileCode AS STRING) AS data_file_code,
        CAST(value.PaymentsOnly AS BOOLEAN) AS payments_only,
        CAST(value.DataFileID AS STRING) AS data_file_id
    FROM json_base,
    UNNEST(value) AS value
    QUALIFY ROW_NUMBER() OVER (PARTITION BY date, account_id ORDER BY date DESC) = 1
)

SELECT * FROM unflatten_and_cast
