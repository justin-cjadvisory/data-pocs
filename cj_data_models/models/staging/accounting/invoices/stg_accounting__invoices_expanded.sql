{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:invoices"]
  )
}}

WITH json_base AS (
    SELECT *
    FROM {{ ref('base_accounting__invoices_expanded') }}
),

unflatten_and_cast AS (
    SELECT
        CAST(value.CurrencyCode AS STRING) AS currency_code,
        CAST(value.UpdatedDateUTC AS TIMESTAMP) AS updated_date_utc,
        CAST(value.TotalTax AS FLOAT64) AS total_tax,
        CAST(value.LineAmountTypes AS STRING) AS line_amount_types,
        CAST(value.Status AS STRING) AS status,
        CAST(value.CISDeduction AS STRING) AS cis_deduction,
        CAST(value.Url AS STRING) AS url,
        CAST(value.Date AS TIMESTAMP) AS date,
        CAST(value.DueDate AS TIMESTAMP) AS due_date,
        CAST(value.Contact.Name AS STRING) AS contact_name,
        CAST(value.Contact.ContactID AS STRING) AS contact_id,
        CAST(value.HasAttachments AS BOOLEAN) AS has_attachments,
        CAST(value.Total AS FLOAT64) AS total,
        CAST(value.PlannedPaymentDate AS STRING) AS planned_payment_date,
        CAST(value.CurrencyRate AS INT64) AS currency_rate,
        CAST(value.FullyPaidOnDate AS TIMESTAMP) AS fully_paid_on_date,
        CAST(value.ExpectedPaymentDate AS STRING) AS expected_payment_date,
        CAST(value.SubTotal AS FLOAT64) AS sub_total,
        CAST(value.AmountCredited AS INT64) AS amount_credited,
        CAST(value.AmountDue AS INT64) AS amount_due,
        CAST(value.RepeatingInvoiceID AS STRING) AS repeating_invoice_id,
        CAST(value.BrandingThemeID AS STRING) AS branding_theme_id,
        CAST(value.AmountPaid AS FLOAT64) AS amount_paid,
        CAST(value.Reference AS STRING) AS reference,
        CAST(value.SentToContact AS BOOLEAN) AS sent_to_contact,
        CAST(value.InvoiceNumber AS STRING) AS invoice_number,
        CAST(value.InvoiceID AS STRING) AS invoice_id,
        CAST(value.Type AS STRING) AS type,
        CAST(value.DataFile.DataFileCode AS STRING) AS data_file_code,
        CAST(value.DataFile.DataFileName AS STRING) AS data_file_name,
        CAST(value.DataFile.DataFileID AS STRING) AS data_file_id,
        -- Unnest Payments
        CAST(payment.Date AS TIMESTAMP) AS payment_date,
        CAST(payment.CurrencyRate AS INT64) AS payment_currency_rate,
        CAST(payment.Reference AS STRING) AS payment_reference,
        CAST(payment.BatchPaymentID AS STRING) AS batch_payment_id,
        CAST(payment.Amount AS FLOAT64) AS payment_amount,
        CAST(payment.PaymentID AS STRING) AS payment_id,
        -- Unnest LineItems
        CAST(line_item.LineItemID AS STRING) AS line_item_id,
        CAST(line_item.AccountID AS STRING) AS line_item_account_id,
        CAST(line_item.Item AS STRING) AS line_item_item,
        CAST(line_item.TaxType AS STRING) AS line_item_tax_type,
        CAST(line_item.DiscountRate AS STRING) AS line_item_discount_rate,
        CAST(line_item.LineAmount AS FLOAT64) AS line_item_line_amount,
        CAST(line_item.AccountCode AS INT64) AS line_item_account_code,
        CAST(line_item.TaxAmount AS FLOAT64) AS line_item_tax_amount,
        CAST(line_item.UnitAmount AS FLOAT64) AS line_item_unit_amount,
        CAST(line_item.Quantity AS INT64) AS line_item_quantity,
        CAST(line_item.Description AS STRING) AS line_item_description,
        CAST(line_item.ItemCode AS STRING) AS line_item_item_code,
        -- Unnest LineItems.Tracking
        CAST(tracking.TrackingCategoryID AS STRING) AS tracking_category_id,
        CAST(tracking.Option AS STRING) AS tracking_option,
        CAST(tracking.Name AS STRING) AS tracking_name
    FROM
      json_base,
      UNNEST(value) AS value,
      UNNEST(value.Payments) AS payment,
      UNNEST(value.LineItems) AS line_item,
      UNNEST(line_item.Tracking) AS tracking
)

SELECT * 
FROM unflatten_and_cast
QUALIFY ROW_NUMBER() OVER (PARTITION BY updated_date_utc, invoice_id ORDER BY updated_date_utc DESC) = 1