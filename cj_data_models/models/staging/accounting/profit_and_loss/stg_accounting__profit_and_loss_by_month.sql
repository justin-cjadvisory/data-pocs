{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:profit_and_loss"]
  )
}}

WITH json_base AS (
    SELECT *
    FROM  {{ ref('base_accounting__profit_and_loss_by_month') }}
),

unflatten_and_cast AS (
    SELECT
        CAST(amount.Amount AS FLOAT64) AS amount,
        CAST(amount.Date AS DATE) AS amount_date,
        CAST(line.AccountName AS STRING) AS account_name,
        CAST(line.AccountID AS STRING) AS account_id,
        CAST(line.AccountLineType AS STRING) AS account_line_type,
        CAST(line.LineType AS STRING) AS line_type,
        CAST(val.StandardLayout AS BOOLEAN) AS standard_layout,
        CAST(val.UpdatedDateUTC AS TIMESTAMP) AS updated_date_utc,
        CAST(val.PaymentsOnly AS BOOLEAN) AS payments_only,
        CAST(val.FinancialYear AS INT64) AS financial_year,
        CAST(val.DataFile.DataFileCode AS STRING) AS datafile_code,
        CAST(val.DataFile.DataFileName AS STRING) AS datafile_name,
        CAST(val.DataFile.DataFileID AS STRING) AS datafile_id
    FROM json_base,
    UNNEST(value) AS val,
    UNNEST(val.Lines) AS line,
    UNNEST(line.Amounts) AS amount
)

SELECT * FROM unflatten_and_cast
WHERE line_type != 'Total'
AND amount_date <= LAST_DAY(CURRENT_DATE())
