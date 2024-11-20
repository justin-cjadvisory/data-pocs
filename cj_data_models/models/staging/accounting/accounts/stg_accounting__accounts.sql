{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:accounts"]
  )
}}

WITH json_base AS (
    SELECT *
    FROM  {{ ref('base_accounting__accounts') }}
),

unflatten_and_cast AS (
    SELECT
        CAST(val.UpdatedDateUTC AS TIMESTAMP) AS updated_date_utc,
        CAST(val.ReportingCodeName AS STRING) AS reporting_code_name,
        CAST(val.DataFile.DataFileCode AS STRING) AS data_file_code,
        CAST(val.DataFile.DataFileName AS STRING) AS data_file_name,
        CAST(val.DataFile.DataFileID AS STRING) AS data_file_id,
        CAST(val.Code AS STRING) AS code,
        CAST(val.Status AS STRING) AS status,
        CAST(val.Name AS STRING) AS name,
        CAST(val.AddToWatchlist AS BOOLEAN) AS add_to_watchlist,
        CAST(val.CurrencyCode AS STRING) AS currency_code,
        CAST(val.BankAccountType AS STRING) AS bank_account_type,
        CAST(val.EnablePaymentsToAccount AS BOOLEAN) AS enable_payments_to_account,
        CAST(val.Type AS STRING) AS type,
        CAST(val.Class AS STRING) AS class,
        CAST(val.HasAttachments AS BOOLEAN) AS has_attachments,
        CAST(val.SystemAccount AS STRING) AS system_account,
        CAST(val.Description AS STRING) AS description,
        CAST(val.ShowInExpenseClaims AS BOOLEAN) AS show_in_expense_claims,
        CAST(val.TaxType AS STRING) AS tax_type,
        CAST(val.BankAccountNumber AS STRING) AS bank_account_number,
        CAST(val.AccountID AS STRING) AS account_id
    FROM json_base, 
    UNNEST(value) AS val
)

SELECT * FROM unflatten_and_cast
