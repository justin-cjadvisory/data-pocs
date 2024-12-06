{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:overpayments"]
  )
}}

WITH json_base AS (
    SELECT *
    FROM {{ ref('base_accounting__overpayments_expanded') }}  -- Replace with your base table
),

unflatten_and_cast AS (
    SELECT
        -- Currency fields
        CAST(value.CurrencyRate AS INT64) AS currency_rate,
        CAST(value.CurrencyCode AS STRING) AS currency_code,
        CAST(value.TotalTax AS INT64) AS total_tax,
        
        -- LineItems (repeated)
        li.LineItemID AS line_item_id,
        li.AccountID AS line_item_account_id,
        li.TaxType AS line_item_tax_type,
        CAST(li.LineAmount AS FLOAT64) AS line_item_amount,
        li.AccountCode AS line_item_account_code,
        CAST(li.TaxAmount AS INT64) AS line_item_tax_amount,
        CAST(li.UnitAmount AS FLOAT64) AS line_item_unit_amount,
        li.Description AS line_item_description,
        
        -- Tracking (repeated within LineItems)
        tracking.TrackingCategoryID AS tracking_category_id,
        tracking.Option AS tracking_option,
        tracking.Name AS tracking_name,
        
        -- Status and Date fields
        CAST(value.Status AS STRING) AS status,
        CAST(value.UpdatedDateUTC AS TIMESTAMP) AS updated_date_utc,
        CAST(value.Date AS TIMESTAMP) AS date,
        
        -- Payment fields (repeated)
        p.Date AS payment_date,
        CAST(p.CurrencyRate AS INT64) AS payment_currency_rate,
        p.Reference AS payment_reference,
        p.BatchPaymentID AS payment_batch_payment_id,
        CAST(p.Amount AS FLOAT64) AS payment_amount,
        p.PaymentID AS payment_id,
        
        -- Contact (structured data, not unnested)
        CAST(value.Contact.Name AS STRING) AS contact_name,
        CAST(value.Contact.ContactID AS STRING) AS contact_id,
        
        -- Total and Reference fields
        CAST(value.Total AS FLOAT64) AS total,
        CAST(value.Reference AS STRING) AS reference,
        
        -- Allocations (repeated)
        alloc.Date AS allocation_date,
        CAST(alloc.Amount AS FLOAT64) AS allocation_amount,
        alloc.AllocationID AS allocation_id,
        
        -- Invoice (record within Allocations)
        alloc.Invoice.InvoiceNumber AS invoice_number,
        alloc.Invoice.InvoiceID AS invoice_id,
        
        -- Unnesting InvoicePaymentServices and InvoiceAddresses within Allocations
        alloc.Invoice.InvoicePaymentServices AS invoice_payment_services,
        alloc.Invoice.InvoiceAddresses AS invoice_addresses,
        
        -- Remaining Credit and Overpayment
        CAST(value.RemainingCredit AS FLOAT64) AS remaining_credit,
        CAST(value.OverpaymentID AS STRING) AS overpayment_id,
        
        -- Type and SubTotal fields
        CAST(value.Type AS STRING) AS type,
        CAST(value.SubTotal AS FLOAT64) AS subtotal,
        
        -- DataFile (record)
        CAST(value.DataFile.DataFileCode AS STRING) AS data_file_code,
        CAST(value.DataFile.DataFileName AS STRING) AS data_file_name,
        CAST(value.DataFile.DataFileID AS STRING) AS data_file_id

    FROM json_base,
    UNNEST(value) AS value,
    
    -- Unnesting LineItems and its Tracking
    UNNEST(value.LineItems) AS li,
    UNNEST(li.Tracking) AS tracking,

    -- Unnesting Payments
    UNNEST(value.Payments) AS p,

    -- Unnesting Allocations
    UNNEST(value.Allocations) AS alloc
)

SELECT * FROM unflatten_and_cast
QUALIFY ROW_NUMBER() OVER (PARTITION BY updated_date_utc, line_item_account_id ORDER BY updated_date_utc DESC) = 1