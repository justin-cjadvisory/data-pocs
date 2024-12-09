{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:invoices"]
  )
}}

WITH json_base AS (
    SELECT *
    FROM {{ ref('base_accounting__invoices_expanded_table') }}
),

unflatten_and_cast AS (
    SELECT
        CAST(value.Url AS STRING) AS url,
        CAST(value.LineUnitAmount AS FLOAT64) AS line_unit_amount,
        CAST(value.TrackingOption1Name AS STRING) AS tracking_option_1_name,
        CAST(value.CISDeduction AS STRING) AS cis_deduction,
        CAST(value.CurrencyCode AS STRING) AS currency_code,
        CAST(value.LineTaxType AS STRING) AS line_tax_type,
        CAST(value.AmountPaid AS FLOAT64) AS amount_paid,
        CAST(value.LineID AS STRING) AS line_id,
        CAST(value.TotalTax AS FLOAT64) AS total_tax,
        CAST(value.AmountCredited AS INTEGER) AS amount_credited,
        CAST(value.SubTotal AS FLOAT64) AS sub_total,
        CAST(value.Type AS STRING) AS type,
        CAST(value.HasAttachments AS BOOLEAN) AS has_attachments,
        CAST(value.TrackingCategory2Name AS STRING) AS tracking_category_2_name,
        CAST(value.TrackingCategory1ID AS STRING) AS tracking_category_1_id,
        CAST(value.LineItemName AS STRING) AS line_item_name,
        CAST(value.LineItemCode AS STRING) AS line_item_code,
        CAST(value.LineItemID AS STRING) AS line_item_id,
        CAST(value.LineAmount AS FLOAT64) AS line_amount,
        CAST(value.LineDescription AS STRING) AS line_description,
        CAST(value.LineQuantity AS INTEGER) AS line_quantity,
        CAST(value.LineAccountID AS STRING) AS line_account_id,
        CAST(value.LineAmountTypes AS STRING) AS line_amount_types,
        CAST(value.CurrencyRate AS INTEGER) AS currency_rate,
        CAST(value.Total AS FLOAT64) AS total,
        CAST(value.SentToContact AS BOOLEAN) AS sent_to_contact,
        CAST(value.TrackingOption2Name AS STRING) AS tracking_option_2_name,
        CAST(value.Reference AS STRING) AS reference,
        CAST(value.PlannedPaymentDate AS STRING) AS planned_payment_date,
        CAST(value.LineTaxAmount AS FLOAT64) AS line_tax_amount,
        CAST(value.LineDiscountRate AS STRING) AS line_discount_rate,
        CAST(value.DueDate AS DATE) AS due_date,
        CAST(value.Date AS DATE) AS date,
        CAST(value.ExpectedPaymentDate AS STRING) AS expected_payment_date,
        CAST(value.Status AS STRING) AS status,
        CAST(value.UpdatedDateUTC AS TIMESTAMP) AS updated_date_utc,
        CAST(value.RepeatingInvoiceID AS STRING) AS repeating_invoice_id,
        CAST(value.AmountDue AS INTEGER) AS amount_due,
        CAST(value.ContactID AS STRING) AS contact_id,
        CAST(value.FullyPaidOnDate AS DATE) AS fully_paid_on_date,
        CAST(value.BrandingThemeID AS STRING) AS branding_theme_id,
        CAST(value.TrackingCategory1Name AS STRING) AS tracking_category_1_name,
        CAST(value.InvoiceID AS STRING) AS invoice_id,
        CAST(value.DataFileID AS STRING) AS data_file_id,
        CAST(value.DataFileCode AS STRING) AS data_file_code,
        CAST(value.DataFileName AS STRING) AS data_file_name,
        CAST(value.TrackingCategory2ID AS STRING) AS tracking_category_2_id,
        CAST(value.ContactName AS STRING) AS contact_name,
        CAST(value.LineAccountCode AS INTEGER) AS line_account_code,
        CAST(value.InvoiceNumber AS STRING) AS invoice_number
    FROM json_base,
    UNNEST(value) AS value
)

SELECT * FROM unflatten_and_cast
QUALIFY ROW_NUMBER() OVER (PARTITION BY updated_date_utc, invoice_id ORDER BY updated_date_utc DESC) = 1