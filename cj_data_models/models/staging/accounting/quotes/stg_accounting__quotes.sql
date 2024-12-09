{{ config(
    materialized='view',
    tags=["module:accounting", "submodule:quotes"]
) }}

WITH json_base AS (
    SELECT *
    FROM {{ ref('base_accounting__quotes') }}  -- Replace with your base table
),

unflatten_and_cast AS (
    SELECT 
        -- Basic fields
        CAST(value.BrandingThemeID AS STRING) AS branding_theme_id,
        CAST(value.CurrencyCode AS STRING) AS currency_code,
        CAST(value.Status AS STRING) AS status,
        CAST(value.UpdatedDateUTC AS TIMESTAMP) AS updated_date_utc,
        CAST(value.Summary AS STRING) AS summary,
        CAST(value.Date AS TIMESTAMP) AS date,
        CAST(value.LineAmountTypes AS STRING) AS line_amount_types,
        CAST(value.ExpiryDate AS TIMESTAMP) AS expiry_date,
        
        -- Contact information
        CAST(value.Contact.LastName AS STRING) AS contact_last_name,
        CAST(value.Contact.FirstName AS STRING) AS contact_first_name,
        CAST(value.Contact.Name AS STRING) AS contact_name,
        CAST(value.Contact.EmailAddress AS STRING) AS contact_email_address,
        CAST(value.Contact.ContactID AS STRING) AS contact_id,
        CAST(value.Terms AS STRING) AS terms,
        CAST(value.QuoteNumber AS STRING) AS quote_number,
        CAST(value.QuoteID AS STRING) AS quote_id,
        CAST(value.Title AS STRING) AS title,
        
        -- Financial fields
        CAST(value.CurrencyRate AS INT64) AS currency_rate,
        CAST(value.TotalTax AS FLOAT64) AS total_tax,
        
        -- LineItems REPEATED field (corrected)
        CAST(line_items.LineItemID AS STRING) AS line_item_id,
        CAST(line_items.ItemCode AS STRING) AS item_code,
        CAST(line_items.DiscountRate AS FLOAT64) AS discount_rate,
        CAST(line_items.TaxType AS STRING) AS tax_type,
        CAST(line_items.LineAmount AS FLOAT64) AS line_amount,
        CAST(line_items.DiscountAmount AS FLOAT64) AS discount_amount,
        CAST(line_items.UnitAmount AS FLOAT64) AS unit_amount,
        CAST(line_items.Quantity AS FLOAT64) AS quantity,
        CAST(line_items.Description AS STRING) AS description,
        CAST(line_items.TaxAmount AS FLOAT64) AS tax_amount,
        CAST(line_items.AccountCode AS INT64) AS account_code,
        
        -- Nested LineItems.Tracking REPEATED field
        CAST(line_items_tracking.Option AS STRING) AS tracking_option,
        CAST(line_items_tracking.Name AS STRING) AS tracking_name,
        CAST(line_items_tracking.TrackingOptionID AS STRING) AS tracking_option_id,
        CAST(line_items_tracking.TrackingCategoryID AS STRING) AS tracking_category_id,
        
        CAST(value.Total AS FLOAT64) AS total,
        CAST(value.Reference AS STRING) AS reference,
        CAST(value.TotalDiscount AS FLOAT64) AS total_discount,
        CAST(value.SubTotal AS FLOAT64) AS sub_total,
        
        -- DataFile information
        CAST(value.DataFile.DataFileCode AS STRING) AS data_file_code,
        CAST(value.DataFile.DataFileName AS STRING) AS data_file_name,
        CAST(value.DataFile.DataFileID AS STRING) AS data_file_id

    FROM json_base,
    UNNEST(value) AS value,

    -- Unnest nested LineItems and LineItems.Tracking
    UNNEST(value.LineItems) AS line_items,
    UNNEST(line_items.Tracking) AS line_items_tracking
    QUALIFY ROW_NUMBER() OVER (PARTITION BY updated_date_utc, quote_id ORDER BY updated_date_utc DESC) = 1
)

SELECT * 
FROM unflatten_and_cast
