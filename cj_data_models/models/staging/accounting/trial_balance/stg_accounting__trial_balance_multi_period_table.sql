{{ config(
    materialized='view',
    tags=["module:accounting", "submodule:trial_balance"]
) }}

WITH base_data AS (
    SELECT *
    FROM {{ ref('base_accounting__trial_balance_multi_period_table') }}  -- Replace with your base model reference
),

unnested_data AS (
    SELECT 
        -- Extract top-level fields
        main.DebitAmountYTD AS debit_amount_ytd,
        main.CreditAmount AS credit_amount,
        main.AccountLineType AS account_line_type,
        main.CreditAmountYTD AS credit_amount_ytd,
        main.DataFileCode AS data_file_code,
        main.DataFileID AS data_file_id,
        main.DebitAmount AS debit_amount,
        main.AccountID AS account_id,
        main.LineType AS line_type,
        main.PaymentsOnly AS payments_only,
        main.Period AS period,
        main.AccountName AS account_name,
        main.Date AS record_date,
        main.DataFileName AS data_file_name
    FROM base_data,
    UNNEST(value) AS main  -- Unnest `value` field
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id, record_date ORDER BY record_date DESC) = 1
)

SELECT *
FROM unnested_data
