{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:banking"]
  )
}}

WITH json_base AS (
    SELECT *
    FROM {{ ref('base_accounting__batch_payments') }}
), 

unflatten_and_cast AS (
    SELECT
        CAST(value.Details AS STRING) AS details,
        CAST(value.IsReconciled AS BOOLEAN) AS is_reconciled,
        CAST(value.Particulars AS STRING) AS particulars,
        CAST(value.TotalAmount AS FLOAT64) AS total_amount,
        CAST(value.BatchPaymentID AS STRING) AS batch_payment_id,
        CAST(value.Status AS STRING) AS status,
        CAST(payment.BankAccountNumber AS INT64) AS payment_bank_account_number,
        CAST(payment.Details AS STRING) AS payment_details,
        CAST(payment.Amount AS FLOAT64) AS payment_amount,
        CAST(payment.PaymentID AS STRING) AS payment_id,
        CAST(payment.Invoice.InvoiceID AS STRING) AS invoice_id, 
        ARRAY_TO_STRING(payment.Invoice.InvoicePaymentServices, ', ') AS invoice_payment_services,
        ARRAY_TO_STRING(payment.Invoice.InvoiceAddresses, ', ') AS invoice_addresses,
        CAST(value.UpdatedDateUTC AS TIMESTAMP) AS updated_date_utc,
        CAST(value.Date AS TIMESTAMP) AS date,
        CAST(value.Narrative AS STRING) AS narrative,
        CAST(value.Type AS STRING) AS type,
        CAST(value.DataFile.DataFileCode AS STRING) AS data_file_code,
        CAST(value.DataFile.DataFileName AS STRING) AS data_file_name,
        CAST(value.DataFile.DataFileID AS STRING) AS data_file_id,
        CAST(value.Account.AccountID AS STRING) AS account_id,
        CAST(value.Reference AS STRING) AS reference
    FROM json_base,
    UNNEST(value) AS value,
    UNNEST(value.Payments) AS payment  
    QUALIFY ROW_NUMBER() OVER (PARTITION BY updated_date_utc, account_id ORDER BY updated_date_utc DESC) = 1
)

SELECT * FROM unflatten_and_cast
