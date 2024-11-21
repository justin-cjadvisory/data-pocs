{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:balance_sheets"]
  )
}}

WITH json_base AS (
    SELECT *
    FROM  {{ ref('base_accounting__balance_sheet_by_month') }}
),

unflatten_and_cast AS (
    SELECT
        CAST(amounts.Amount AS FLOAT64) AS amount,
        CAST(amounts.Date AS DATE) AS amount_date,
        CAST(lines.AccountName AS STRING) AS account_name,
        CAST(lines.AccountID AS STRING) AS account_id,
        CAST(lines.AccountLineType AS STRING) AS account_line_type,
        CAST(lines.LineType AS STRING) AS line_type,
        CAST(value.StandardLayout AS BOOLEAN) AS standard_layout,
        CAST(value.UpdatedDateUTC AS TIMESTAMP) AS updated_date_utc,
        CAST(value.PaymentsOnly AS BOOLEAN) AS payments_only,
        CAST(value.FinancialYear AS INT64) AS financial_year,
        CAST(value.DataFile.DataFileCode AS STRING) AS data_file_code,
        CAST(value.DataFile.DataFileName AS STRING) AS data_file_name,
        CAST(value.DataFile.DataFileID AS STRING) AS data_file_id
    FROM json_base,
    UNNEST(value) AS value,
    UNNEST(value.Lines) AS lines,
    UNNEST(lines.Amounts) AS amounts
)

SELECT * FROM unflatten_and_cast
