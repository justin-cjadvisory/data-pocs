{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:payments"]
  )
}}

WITH json_base AS (
    SELECT *
    FROM {{ ref('base_accounting__payments') }}  -- Replace with your base table
),

unflatten_and_cast AS (
    SELECT 
        -- Fields from root record
        CAST(value.Date AS TIMESTAMP) AS date,
        CAST(value.UpdatedDateUTC AS TIMESTAMP) AS updated_date_utc,
        CAST(value.CurrencyRate AS INT64) AS currency_rate,
        CAST(value.IsReconciled AS BOOLEAN) AS is_reconciled,
        CAST(value.PaymentType AS STRING) AS payment_type,
        CAST(value.BatchPayment AS STRING) AS batch_payment,
        CAST(value.Reference AS STRING) AS reference,
        CAST(value.Status AS STRING) AS status,
        CAST(value.PaymentID AS STRING) AS payment_id,
        CAST(value.Amount AS FLOAT64) AS amount,

        -- Fields from Invoice struct (no unnesting, just field extraction)
        CAST(value.Invoice.InvoiceNumber AS STRING) AS invoice_number,
        CAST(value.Invoice.InvoiceID AS STRING) AS invoice_id,
        CAST(value.Invoice.CurrencyCode AS STRING) AS invoice_currency_code,
        CAST(value.Invoice.Type AS STRING) AS invoice_type,

        -- Unnesting Invoice's Contact struct
        CAST(value.Invoice.Contact.Name AS STRING) AS contact_name,
        CAST(value.Invoice.Contact.ContactID AS STRING) AS contact_id,

        -- Unnesting Invoice's InvoicePaymentServices (ARRAY)
        ips AS invoice_payment_services,  -- The array is unnested here

        -- Unnesting Invoice's InvoiceAddresses (ARRAY)
        ias AS invoice_addresses,        -- The array is unnested here

        -- Unnesting Account fields (if Account is a RECORD with fields)
        CAST(value.Account.Code AS INT64) AS account_code,
        CAST(value.Account.AccountID AS STRING) AS account_id,

        -- Unnesting DataFile fields (if DataFile is a RECORD with fields)
        CAST(value.DataFile.DataFileCode AS STRING) AS data_file_code,
        CAST(value.DataFile.DataFileName AS STRING) AS data_file_name,
        CAST(value.DataFile.DataFileID AS STRING) AS data_file_id

    FROM json_base,
    UNNEST(value) AS value,                         -- Unnest the main record array
    UNNEST(value.Invoice.InvoicePaymentServices) AS ips,  -- Unnest InvoicePaymentServices (ARRAY)
    UNNEST(value.Invoice.InvoiceAddresses) AS ias,        -- Unnest InvoiceAddresses (ARRAY)
    UNNEST([value.Account]) AS account                     -- Flatten Account (if it's a single record)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY updated_date_utc, account_id ORDER BY updated_date_utc DESC) = 1
)

SELECT * FROM unflatten_and_cast
