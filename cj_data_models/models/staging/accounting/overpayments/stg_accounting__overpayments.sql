{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:overpayments"]
  )
}}

WITH json_base AS (
    SELECT *
    FROM {{ ref('base_accounting__overpayments') }}  -- Replace with your base table
),

unflatten_and_cast AS (
    SELECT 
        -- Value record fields
        CAST(value.CurrencyRate AS INT64) AS currency_rate,  -- BigQuery INT64
        CAST(value.CurrencyCode AS STRING) AS currency_code,  -- BigQuery STRING
        CAST(value.TotalTax AS INT64) AS total_tax,  -- BigQuery INT64
        CAST(value.Status AS STRING) AS status,  -- BigQuery STRING
        CAST(value.UpdatedDateUTC AS TIMESTAMP) AS updated_date_utc,  -- BigQuery TIMESTAMP
        CAST(value.Date AS TIMESTAMP) AS date,  -- BigQuery TIMESTAMP
        CAST(value.HasAttachments AS BOOL) AS has_attachments,  -- BigQuery BOOL
        
        -- Payments (repeated)
        pmt.Date AS payment_date,  -- BigQuery TIMESTAMP
        CAST(pmt.CurrencyRate AS INT64) AS payment_currency_rate,  -- BigQuery INT64
        CAST(pmt.Reference AS STRING) AS payment_reference,  -- BigQuery STRING
        CAST(pmt.BatchPaymentID AS STRING) AS batch_payment_id,  -- BigQuery STRING
        CAST(pmt.Amount AS FLOAT64) AS payment_amount,  -- BigQuery FLOAT64
        CAST(pmt.PaymentID AS STRING) AS payment_id,  -- BigQuery STRING
        
        -- Contact (record)
        CAST(value.Contact.Name AS STRING) AS contact_name,  -- BigQuery STRING
        CAST(value.Contact.ContactID AS STRING) AS contact_id,  -- BigQuery STRING
        
        -- Other fields from value record
        CAST(value.Total AS FLOAT64) AS total,  -- BigQuery FLOAT64
        CAST(value.Reference AS STRING) AS reference,  -- BigQuery STRING
        
        -- Allocations (repeated)
        alloc.Date AS allocation_date,  -- BigQuery TIMESTAMP
        CAST(alloc.Amount AS FLOAT64) AS allocation_amount,  -- BigQuery FLOAT64
        CAST(alloc.AllocationID AS STRING) AS allocation_id,  -- BigQuery STRING
        
        -- Invoice (nested in Allocations)
        alloc.Invoice.InvoiceNumber AS invoice_number,  -- BigQuery STRING
        alloc.Invoice.InvoiceID AS invoice_id,  -- BigQuery STRING
        
        -- Unnesting InvoicePaymentServices (repeated array of strings)
        payment_service AS invoice_payment_service,  -- BigQuery STRING
        
        -- Unnesting InvoiceAddresses (repeated array of strings)
        invoice_address AS invoice_address,  -- BigQuery STRING (array of strings)
        
        -- Remaining Credit & Overpayment Info
        CAST(value.RemainingCredit AS FLOAT64) AS remaining_credit,  -- BigQuery FLOAT64
        CAST(value.OverpaymentID AS STRING) AS overpayment_id,  -- BigQuery STRING
        
        -- Type and Subtotal fields
        CAST(value.Type AS STRING) AS type,  -- BigQuery STRING
        CAST(value.SubTotal AS FLOAT64) AS sub_total,  -- BigQuery FLOAT64
        
        -- DataFile fields (record)
        CAST(value.DataFile.DataFileCode AS STRING) AS data_file_code,  -- BigQuery STRING
        CAST(value.DataFile.DataFileName AS STRING) AS data_file_name,  -- BigQuery STRING
        CAST(value.DataFile.DataFileID AS STRING) AS data_file_id  -- BigQuery STRING

    FROM json_base,
    UNNEST(value) AS value,  -- Unnesting the value array
    UNNEST(value.Payments) AS pmt,  -- Unnesting Payments
    UNNEST(value.Allocations) AS alloc,  -- Unnesting Allocations
    UNNEST(alloc.Invoice.InvoiceAddresses) AS invoice_address,  -- Unnesting InvoiceAddresses (repeated string)
    UNNEST(alloc.Invoice.InvoicePaymentServices) AS payment_service  -- Unnesting InvoicePaymentServices (repeated string)
)

SELECT * FROM unflatten_and_cast
