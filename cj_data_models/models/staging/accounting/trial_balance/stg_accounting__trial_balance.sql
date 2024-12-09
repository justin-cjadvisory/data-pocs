{{ config(
    materialized='view',
    tags=["module:accounting", "submodule:trial_balance"]
) }}

WITH base_data AS (
    SELECT *
    FROM {{ ref('base_accounting__trial_balance') }}  -- Replace with your base model reference
),

unnested_data AS (
    SELECT 
        -- Extract top-level fields
        main.UpdatedDateUTC AS updated_date_utc,
        main.PaymentsOnly AS payments_only,
        main.Date AS record_date,

        -- Extract fields from DataFile record
        main.DataFile.DataFileCode AS data_file_code,
        main.DataFile.DataFileName AS data_file_name,
        main.DataFile.DataFileID AS data_file_id,

        -- Unnest Lines
        line.CreditAmountYTD AS credit_amount_ytd,
        line.CreditAmount AS credit_amount,
        line.DebitAmount AS debit_amount,
        line.AccountName AS account_name,
        line.AccountID AS account_id,
        line.AccountLineType AS account_line_type,
        line.DebitAmountYTD AS debit_amount_ytd,
        line.LineType AS line_type
    FROM base_data,
    UNNEST(value) AS main,       -- Unnest `value` field
    UNNEST(main.Lines) AS line   -- Unnest `Lines` within `value`
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id, record_date ORDER BY record_date DESC) = 1
)

SELECT *
FROM unnested_data
