{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:contacts"]
  )
}}

WITH json_base AS (
    SELECT *
    FROM {{ ref('base_accounting__contacts_expanded') }}
),

unflatten_and_cast AS  (
    SELECT
        -- Basic Fields
        CAST(value.BrandingTheme AS STRING) AS branding_theme,
        CAST(value.Discount AS STRING) AS discount,
        CAST(value.IsSupplier AS BOOLEAN) AS is_supplier,
        CAST(value.DefaultCurrency AS STRING) AS default_currency,
        CAST(value.IsCustomer AS BOOLEAN) AS is_customer,
        CAST(value.UpdatedDateUTC AS TIMESTAMP) AS updated_date_utc,
        CAST(value.BatchPayments AS STRING) AS batch_payments,
        CAST(value.PurchasesDefaultAccountCode AS STRING) AS purchases_default_account_code,
        CAST(value.Website AS STRING) AS website,
        CAST(value.TaxNumber AS STRING) AS tax_number,
        CAST(value.SkypeUserName AS STRING) AS skype_username,
        CAST(value.SalesDefaultAccountCode AS STRING) AS sales_default_account_code,
        CAST(value.EmailAddress AS STRING) AS email_address,
        CAST(value.LastName AS STRING) AS last_name,
        CAST(value.AccountsReceivableTaxType AS STRING) AS accounts_receivable_tax_type,
        CAST(value.SalesDefaultLineAmountType AS STRING) AS sales_default_line_amount_type,
        CAST(value.ContactStatus AS STRING) AS contact_status,
        CAST(value.FirstName AS STRING) AS first_name,
        CAST(value.ContactID AS STRING) AS contact_id,
        CAST(value.Name AS STRING) AS name,
        CAST(value.AccountsPayableTaxType AS STRING) AS accounts_payable_tax_type,
        CAST(value.AccountNumber AS STRING) AS account_number,
        CAST(value.CompanyNumber AS STRING) AS company_number,
        CAST(value.PurchasesDefaultLineAmountType AS STRING) AS purchases_default_line_amount_type,
        CAST(value.ContactNumber AS STRING) AS contact_number,
        CAST(value.BankAccountDetails AS STRING) AS bank_account_details,

        -- Arrays: SalesTrackingCategories and PurchasesTrackingCategories
        ARRAY_TO_STRING(value.SalesTrackingCategories, ', ') AS sales_tracking_categories,
        ARRAY_TO_STRING(value.PurchasesTrackingCategories, ', ') AS purchases_tracking_categories,

        -- Nested Array: Phones
        ARRAY_TO_STRING(
            ARRAY(
                SELECT CONCAT(
                    CAST(phone.PhoneAreaCode AS STRING), 
                    '-', 
                    CAST(phone.PhoneNumber AS STRING), 
                    ' (', 
                    CAST(phone.PhoneCountryCode AS STRING), 
                    ') ',
                    phone.PhoneType
                )
                FROM UNNEST(value.Phones) AS phone
            ),
            ', '
        ) AS phones,

        -- Nested Array: Addresses
        ARRAY_TO_STRING(
            ARRAY(
                SELECT CONCAT(
                    address.Country, ', ', 
                    CAST(address.PostalCode AS STRING), ', ', 
                    address.Region, ', ', 
                    address.City, ', ',
                    address.AddressLine1, ', ', 
                    address.AddressLine2, ', ', 
                    address.AddressLine3, ', ', 
                    address.AddressLine4, ', ',
                    address.AttentionTo, ', ', 
                    address.AddressType
                )
                FROM UNNEST(value.Addresses) AS address
            ),
            ', '
        ) AS addresses,

        -- Nested Object: DataFile
        CAST(value.DataFile.DataFileCode AS STRING) AS data_file_code,
        CAST(value.DataFile.DataFileName AS STRING) AS data_file_name,
        CAST(value.DataFile.DataFileID AS STRING) AS data_file_id,

        -- Repeated Field: ContactGroups
        ARRAY_TO_STRING(value.ContactGroups, ', ') AS contact_groups
    FROM json_base,
    UNNEST(value) AS value  
)

SELECT * FROM unflatten_and_cast