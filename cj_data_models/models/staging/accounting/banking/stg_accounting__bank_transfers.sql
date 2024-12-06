{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:banking"]
  )
}}

WITH json_base AS (
    SELECT *
    FROM {{ ref('base_accounting__bank_transfers') }}
),

unflatten_and_cast AS (
    SELECT
        -- Cast basic fields
        CAST(value.FromIsReconciled AS BOOLEAN) AS from_is_reconciled,
        CAST(value.FromBankTransactionId AS STRING) as from_bank_transaction_id,
        CAST(value.HasAttachments AS BOOLEAN) AS has_attachments,
        CAST(value.Date AS TIMESTAMP) AS date,
        CAST(value.ToBankTransactionID AS STRING) AS to_bank_transaction_id,
        CAST(value.Reference AS STRING) AS reference,
        CAST(value.CreatedDateUTC AS TIMESTAMP) AS created_date_utc,
        CAST(value.BankTransferID AS STRING) AS bank_transfer_id,
        CAST(value.ToIsReconciled AS BOOLEAN) AS to_is_reconciled,
        CAST(value.Amount AS FLOAT64) AS amount,
        CAST(value.ToBankAccount.Code AS STRING) AS to_bank_account_code,
        CAST(value.ToBankAccount.Name AS STRING) AS to_bank_account_name,
        CAST(value.ToBankAccount.AccountID AS STRING) AS to_bank_account_id,
        CAST(value.FromBankAccount.Code AS STRING) AS from_bank_account_code,
        CAST(value.FromBankAccount.Name AS STRING) AS from_bank_account_name,
        CAST(value.FromBankAccount.AccountID AS STRING) AS from_bank_account_id,
        CAST(value.DataFile.DataFileCode AS STRING) AS data_file_code,
        CAST(value.DataFile.DataFileName AS STRING) AS data_file_name,
        CAST(value.DataFile.DataFileID AS STRING) AS data_file_id,
    FROM json_base,
    UNNEST(value) AS value
    QUALIFY ROW_NUMBER() OVER (PARTITION BY created_date_utc, account_id ORDER BY created_date_utc DESC) = 1
)

SELECT * FROM unflatten_and_cast
