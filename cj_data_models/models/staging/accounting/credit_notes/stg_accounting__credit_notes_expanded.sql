{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:credit_notes"]
  )
}}

WITH json_base AS (
    SELECT *
    FROM {{ ref('base_accounting__credit_notes_expanded') }}
),

unflatten_and_cast AS (
    SELECT
        -- Flattened fields from value
        CAST(val.FullyPaidOnDate AS TIMESTAMP) AS fully_paid_on_date,
        CAST(val.TotalTax AS FLOAT64) AS total_tax,
        CAST(ARRAY_TO_STRING(val.InvoiceAddresses, ',') AS STRING) AS invoice_addresses, -- Updated line

        CAST(val.Status AS STRING) AS status,
        CAST(val.HasAttachments AS BOOLEAN) AS has_attachments,
        CAST(val.CurrencyRate AS INT64) AS currency_rate,
        CAST(val.Total AS FLOAT64) AS total,
        CAST(val.SentToContact AS STRING) AS sent_to_contact,
        CAST(val.Reference AS STRING) AS reference,
        CAST(val.LineAmountTypes AS STRING) AS line_amount_types,
        CAST(val.RemainingCredit AS FLOAT64) AS remaining_credit,
        CAST(val.Type AS STRING) AS type,
        CAST(val.CurrencyCode AS STRING) AS currency_code,
        CAST(val.Date AS TIMESTAMP) AS credit_note_date,
        CAST(val.DueDate AS TIMESTAMP) AS due_date,
        CAST(val.CreditNoteID AS STRING) AS credit_note_id,
        CAST(val.BrandingThemeID AS STRING) AS branding_theme_id,
        CAST(val.UpdatedDateUTC AS TIMESTAMP) AS updated_date_utc,
        CAST(val.CreditNoteNumber AS STRING) AS credit_note_number,
        CAST(val.SubTotal AS FLOAT64) AS sub_total,
        CAST(val.Contact.Name AS STRING) AS contact_name,
        CAST(val.Contact.ContactID AS STRING) AS contact_id,
        CAST(datafile.DataFileCode AS STRING) AS datafile_code,
        CAST(datafile.DataFileName AS STRING) AS datafile_name,
        CAST(datafile.DataFileID AS STRING) AS datafile_id,

        -- Fields from the Allocations repeated field
        CAST(alloc.Date AS TIMESTAMP) AS allocation_date,
        CAST(alloc.Amount AS FLOAT64) AS allocation_amount,
        CAST(alloc.AllocationID AS STRING) AS allocation_id,

        -- UNNEST Invoice array to access nested fields
        alloc.Invoice.InvoiceID AS invoice_id,
        CAST(ARRAY_TO_STRING(alloc.Invoice.InvoicePaymentServices, ',') AS STRING) AS invoice_payment_services,
        CAST(ARRAY_TO_STRING(alloc.Invoice.InvoiceAddresses, ',') AS STRING) AS agg_invoice_addresses,

        -- UNNEST LineItems and its nested Tracking field
        CAST(line_item.LineItemID AS STRING) AS line_item_id,
        CAST(line_item.AccountID AS STRING) AS account_id,
        CAST(line_item.Item AS STRING) AS item,
        CAST(line_item.LineAmount AS FLOAT64) AS line_amount,
        CAST(line_item.TaxType AS STRING) AS line_tax_type,
        CAST(line_item.AccountCode AS STRING) AS line_account_code,
        CAST(line_item.TaxAmount AS FLOAT64) AS line_tax_amount,
        CAST(line_item.UnitAmount AS FLOAT64) AS line_unit_amount,
        CAST(line_item.Quantity AS FLOAT64) AS line_quantity,
        CAST(line_item.Description AS STRING) AS line_description,
        CAST(line_item.ItemCode AS STRING) AS line_item_code,

        -- Unnest Tracking array fields
        CAST(tracking.TrackingCategoryID AS STRING) AS tracking_category_id,
        CAST(tracking.Option AS STRING) AS tracking_option,
        CAST(tracking.Name AS STRING) AS tracking_name

    FROM json_base,
        UNNEST(value) AS val,                   -- Unnest the `value` array into rows
        UNNEST(val.LineItems) AS line_item,     -- Unnest `LineItems` from each `val`
        UNNEST(line_item.Tracking) AS tracking, -- Unnest the `Tracking` array within `LineItems`
        UNNEST(val.Allocations) AS alloc,       -- Unnest the `Allocations` repeated record
        UNNEST([val.DataFile]) AS datafile      -- Use ARRAY brackets to handle nullable fields
)

SELECT *
FROM unflatten_and_cast
