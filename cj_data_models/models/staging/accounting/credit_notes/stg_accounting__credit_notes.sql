{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:credit_notes"]
  )
}}

WITH json_base AS (
    SELECT *
    FROM {{ ref('base_accounting__credit_notes') }}
),

unflatten_and_cast AS (
    SELECT
        -- Flattened fields from `value`
        CAST(val.FullyPaidOnDate AS TIMESTAMP) AS fully_paid_on_date,
        CAST(val.TotalTax AS FLOAT64) AS total_tax,
        CAST(val.Status AS STRING) AS status,
        CAST(val.HasAttachments AS BOOLEAN) AS has_attachments,
        CAST(val.Total AS FLOAT64) AS total,
        CAST(val.Reference AS STRING) AS reference,
        CAST(val.RemainingCredit AS FLOAT64) AS remaining_credit,
        CAST(val.Type AS STRING) AS type,
        CAST(val.CurrencyCode AS STRING) AS currency_code,
        CAST(val.DueDate AS TIMESTAMP) AS due_date,
        CAST(val.CreditNoteID AS STRING) AS credit_note_id,
        CAST(val.BrandingThemeID AS STRING) AS branding_theme_id,
        CAST(val.CurrencyRate AS FLOAT64) AS currency_rate,
        CAST(val.SentToContact AS STRING) AS sent_to_contact,
        CAST(val.UpdatedDateUTC AS TIMESTAMP) AS updated_date_utc,
        CAST(val.Date AS TIMESTAMP) AS credit_note_date,
        CAST(val.CreditNoteNumber AS STRING) AS credit_note_number,
        CAST(val.SubTotal AS FLOAT64) AS sub_total,

        -- Fields from the nested `Contact` structure
        CAST(val.Contact.Name AS STRING) AS contact_name,
        CAST(val.Contact.ContactID AS STRING) AS contact_id,

        -- Fields from the nested `DataFile` structure
        CAST(datafile.DataFileCode AS STRING) AS datafile_code,
        CAST(datafile.DataFileName AS STRING) AS datafile_name,
        CAST(datafile.DataFileID AS STRING) AS datafile_id,

        -- Fields from the `Allocations` repeated field
        CAST(alloc.Date AS TIMESTAMP) AS allocation_date,
        CAST(alloc.Amount AS FLOAT64) AS allocation_amount,
        CAST(alloc.AllocationID AS STRING) AS allocation_id,

        -- Fields from the nested `Invoice` structure
        ARRAY_TO_STRING(alloc.Invoice.InvoicePaymentServices, ', ') AS invoice_payment_services,
        ARRAY_TO_STRING(alloc.Invoice.InvoiceAddresses, ', ') AS invoice_addresses,
        CAST(alloc.Invoice.InvoiceID AS STRING) AS invoice_id

    FROM json_base,
        UNNEST(value) AS val,              -- Unnest the `value` repeated record
        UNNEST(val.Allocations) AS alloc,  -- Unnest the `Allocations` repeated record
        UNNEST([val.DataFile]) AS datafile -- Use ARRAY brackets to handle nullable fields
)

SELECT *
FROM unflatten_and_cast
