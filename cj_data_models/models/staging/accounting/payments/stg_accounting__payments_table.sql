{{ config(
    materialized='view',
    tags=["module:accounting", "submodule:payments"]
) }}

WITH json_base AS (
    SELECT *
    FROM {{ ref('base_accounting__payments_table') }}  -- Replace with your base table
),

unflatten_and_cast AS (
    SELECT 
        -- Basic fields
        CAST(value.UpdatedDateUTC AS TIMESTAMP) AS updated_date_utc,
        CAST(value.IsReconciled AS BOOLEAN) AS is_reconciled,
        CAST(value.CurrencyCode AS STRING) AS currency_code,
        CAST(value.BatchPaymentStatus AS STRING) AS batch_payment_status,
        CAST(value.BatchPaymentType AS STRING) AS batch_payment_type,
        CAST(value.CurrencyRate AS INT64) AS currency_rate,
        CAST(value.Amount AS FLOAT64) AS amount,
        CAST(value.Status AS STRING) AS status,
        CAST(value.BatchPaymentDate AS STRING) AS batch_payment_date,
        CAST(value.InvoiceType AS STRING) AS invoice_type,
        CAST(value.ContactName AS STRING) AS contact_name,
        CAST(value.InvoiceNumber AS STRING) AS invoice_number,
        CAST(value.DataFileCode AS STRING) AS data_file_code,
        CAST(value.BatchPaymentTotalAmount AS STRING) AS batch_payment_total_amount,
        CAST(value.InvoiceID AS STRING) AS invoice_id,
        CAST(value.AccountCode AS INT64) AS account_code,
        CAST(value.DataFileID AS STRING) AS data_file_id,
        CAST(value.ContactID AS STRING) AS contact_id,
        CAST(value.AccountID AS STRING) AS account_id,
        CAST(value.Date AS DATE) AS date,
        CAST(value.Reference AS STRING) AS reference,
        CAST(value.PaymentType AS STRING) AS payment_type,
        CAST(value.DataFileName AS STRING) AS data_file_name,
        CAST(value.BatchPaymentID AS STRING) AS batch_payment_id,
        CAST(value.PaymentID AS STRING) AS payment_id
    FROM json_base,
    UNNEST(value) AS value  -- Unnest the REPEATED field 'value'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY updated_date_utc, account_id ORDER BY updated_date_utc DESC) = 1
)

SELECT * 
FROM unflatten_and_cast
