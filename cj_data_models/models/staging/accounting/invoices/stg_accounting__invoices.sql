{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:invoices"]
  )
}}

WITH json_base AS (
    SELECT *
    FROM {{ ref('base_accounting__invoices') }}
),

unflatten_and_cast AS (
    SELECT
        -- Top-level fields from `value` with casting
        CAST(v.CurrencyCode AS STRING) AS currency_code,
        CAST(v.UpdatedDateUTC AS TIMESTAMP) AS updated_date_utc,
        CAST(v.TotalTax AS FLOAT64) AS total_tax,
        CAST(v.Status AS STRING) AS status,
        CAST(v.CISDeduction AS STRING) AS cis_deduction,
        CAST(v.Url AS STRING) AS url,
        CAST(v.Date AS TIMESTAMP) AS invoice_date,
        CAST(v.DueDate AS TIMESTAMP) AS due_date,
        CAST(v.HasAttachments AS BOOLEAN) AS has_attachments,
        CAST(v.Total AS FLOAT64) AS total_amount,
        CAST(v.PlannedPaymentDate AS TIMESTAMP) AS planned_payment_date,
        CAST(v.CurrencyRate AS INT64) AS currency_rate,
        CAST(v.FullyPaidOnDate AS TIMESTAMP) AS fully_paid_on_date,
        CAST(v.ExpectedPaymentDate AS STRING) AS expected_payment_date,
        CAST(v.SubTotal AS FLOAT64) AS subtotal,
        CAST(v.AmountCredited AS FLOAT64) AS amount_credited,
        CAST(v.AmountDue AS FLOAT64) AS amount_due,
        CAST(v.RepeatingInvoiceID AS STRING) AS repeating_invoice_id,
        CAST(v.BrandingThemeID AS STRING) AS branding_theme_id,
        CAST(v.AmountPaid AS FLOAT64) AS amount_paid,
        CAST(v.Reference AS STRING) AS reference,
        CAST(v.SentToContact AS BOOLEAN) AS sent_to_contact,
        CAST(v.InvoiceNumber AS STRING) AS invoice_number,
        CAST(v.InvoiceID AS STRING) AS invoice_id,
        CAST(v.Type AS STRING) AS invoice_type,

        -- Fields from nested `Contact` with casting
        CAST(v.Contact.Name AS STRING) AS contact_name,
        CAST(v.Contact.ContactID AS STRING) AS contact_id,

        -- Fields from nested `DataFile` with casting
        CAST(v.DataFile.DataFileCode AS STRING) AS datafile_code,
        CAST(v.DataFile.DataFileName AS STRING) AS datafile_name,
        CAST(v.DataFile.DataFileID AS STRING) AS datafile_id,

        -- Flattening `Payments` with casting
        CAST(p.Date AS TIMESTAMP) AS payment_date,
        CAST(p.CurrencyRate AS INT64) AS payment_currency_rate,
        CAST(p.Reference AS STRING) AS payment_reference,
        CAST(p.BatchPaymentID AS STRING) AS batch_payment_id,
        CAST(p.Amount AS FLOAT64) AS payment_amount,
        CAST(p.PaymentID AS STRING) AS payment_id,

        -- Flattening `Overpayments` with casting
        CAST(o.Total AS FLOAT64) AS overpayment_total,
        CAST(o.AppliedAmount AS FLOAT64) AS overpayment_applied_amount,
        CAST(o.Date AS TIMESTAMP) AS overpayment_date,
        CAST(o.OverpaymentID AS STRING) AS overpayment_id,

        -- Flattening `CreditNotes` with casting
        CAST(c.Total AS FLOAT64) AS creditnote_total,
        CAST(c.Date AS TIMESTAMP) AS creditnote_date,
        CAST(c.CreditNoteID AS STRING) AS creditnote_id,
        CAST(c.CreditNoteNumber AS STRING) AS creditnote_number,
        CAST(c.AppliedAmount AS FLOAT64) AS creditnote_applied_amount
    FROM json_base,
        UNNEST(value) AS v,                -- Unnest the repeated `value` field
        UNNEST(v.Payments) AS p,           -- Unnest repeated Payments
        UNNEST(v.Overpayments) AS o,       -- Unnest repeated Overpayments
        UNNEST(v.CreditNotes) AS c         -- Unnest repeated CreditNotes
)

SELECT *
FROM unflatten_and_cast
QUALIFY ROW_NUMBER() OVER (PARTITION BY updated_date_utc, invoice_id ORDER BY updated_date_utc DESC) = 1
