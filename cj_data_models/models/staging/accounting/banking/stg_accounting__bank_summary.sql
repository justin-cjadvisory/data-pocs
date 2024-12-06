{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:banking"]
  )
}}

WITH json_base AS (
    SELECT *
    FROM {{ ref('base_accounting__bank_summary') }}
),

unflatten_and_cast AS (
    SELECT
        CAST(value.UpdatedDateUTC AS TIMESTAMP) AS updated_date_utc,
        CAST(value.ToDate AS DATE) AS to_date,
        CAST(value.FromDate AS DATE) AS from_date,
        CAST(lines.ClosingBalanceAmount AS FLOAT64) AS closing_balance_amount,
        CAST(lines.CashReceivedAmount AS FLOAT64) AS cash_received_amount,
        CAST(lines.AccountName AS STRING) AS account_name,
        CAST(lines.AccountID AS STRING) AS account_id,
        CAST(lines.OpeningBalanceAmount AS FLOAT64) AS opening_balance_amount,
        CAST(lines.FXGainLossAmount AS INT64) AS fx_gain_loss_amount,
        CAST(lines.CashSpentAmount AS FLOAT64) AS cash_spent_amount,
        CAST(lines.LineType AS STRING) AS line_type,
        CAST(value.DataFile.DataFileCode AS STRING) AS data_file_code,
        CAST(value.DataFile.DataFileName AS STRING) AS data_file_name,
        CAST(value.DataFile.DataFileID AS STRING) AS data_file_id
    FROM json_base,
    UNNEST(value) AS value,
    UNNEST(value.Lines) AS lines
    QUALIFY ROW_NUMBER() OVER (PARTITION BY updated_date_utc, account_id ORDER BY updated_date_utc DESC) = 1
)

SELECT * FROM unflatten_and_cast
