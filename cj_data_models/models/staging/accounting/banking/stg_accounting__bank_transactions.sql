{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:banking"]
  )
}}

WITH json_base AS (
    SELECT *
    FROM {{ ref('base_accounting__bank_transactions') }}
),

unflatten_and_cast AS (
    SELECT
        CAST(value.CurrencyCode AS STRING) AS currency_code,
        CAST(value.UpdatedDateUTC AS TIMESTAMP) AS updated_date_utc,
        CAST(value.Date AS TIMESTAMP) AS date,
        CAST(value.OverpaymentID AS STRING) AS overpayment_id,
        CAST(value.Type AS STRING) AS type,
        CAST(value.PrepaymentID AS STRING) AS prepayment_id,
        CAST(value.CurrencyRate AS INT64) AS currency_rate,
        CAST(value.IsReconciled AS BOOLEAN) AS is_reconciled,
        CAST(value.Total AS FLOAT64) AS total,
        CAST(value.Reference AS STRING) AS reference,
        CAST(value.HasAttachments AS BOOLEAN) AS has_attachments,
        CAST(value.TotalTax AS FLOAT64) AS total_tax,
        CAST(value.Url AS STRING) AS url,
        CAST(value.Status AS STRING) AS status,
        CAST(value.BankTransactionID AS STRING) AS bank_transaction_id,
        CAST(value.SubTotal AS FLOAT64) AS sub_total,
        CAST(value.BankAccount.Code AS STRING) AS bank_account_code,
        CAST(value.BankAccount.Name AS STRING) AS bank_account_name,
        CAST(value.BankAccount.AccountID AS STRING) AS bank_account_id,
        CAST(value.BatchPayment.UpdatedDateUTC AS TIMESTAMP) AS batch_payment_updated_date_utc,
        CAST(value.BatchPayment.IsReconciled AS BOOLEAN) AS batch_payment_is_reconciled,
        CAST(value.BatchPayment.TotalAmount AS FLOAT64) AS batch_payment_total_amount,
        CAST(value.BatchPayment.Status AS STRING) AS batch_payment_status,
        CAST(value.BatchPayment.Date AS TIMESTAMP) AS batch_payment_date,
        CAST(value.BatchPayment.Type AS STRING) AS batch_payment_type,
        CAST(value.BatchPayment.BatchPaymentID AS STRING) AS batch_payment_id, 
        CAST(value.BatchPayment.Account.AccountID AS STRING) AS batch_payment_account_id,
        CAST(value.DataFile.DataFileCode AS STRING) AS data_file_code,
        CAST(value.DataFile.DataFileName AS STRING) AS data_file_name,
        CAST(value.DataFile.DataFileID AS STRING) AS data_file_id,
        CAST(value.Contact.Name AS STRING) AS contact_name,
        CAST(value.Contact.ContactID AS STRING) AS contact_id
    FROM json_base,
    UNNEST(value) AS value
    QUALIFY ROW_NUMBER() OVER (PARTITION BY updated_date_utc, account_id ORDER BY updated_date_utc DESC) = 1
)

SELECT * FROM unflatten_and_cast
