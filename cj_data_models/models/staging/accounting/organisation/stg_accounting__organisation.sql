{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:organisation"]
  )
}}

WITH json_base AS (
    SELECT *
    FROM {{ ref('base_accounting__organisation') }}  -- Replace with your base table
),

unflatten_and_cast AS (
    SELECT 
        -- PaymentTerms (record)
        CAST(value.PaymentTerms.Sales AS STRING) AS sales_payment_terms,
        CAST(value.PaymentTerms.Bills AS STRING) AS bills_payment_terms,
        
        -- ExternalLinks (repeated)
        el.Url AS external_link_url,
        el.LinkType AS external_link_type,
        
        -- Addresses (repeated)
        addr.Country AS address_country,
        CAST(addr.PostalCode AS INT64) AS address_postal_code,
        addr.Region AS address_region,
        addr.City AS address_city,
        addr.AddressLine4 AS address_line_4,
        addr.AttentionTo AS address_attention_to,
        addr.AddressLine3 AS address_line_3,
        addr.AddressLine2 AS address_line_2,
        addr.AddressLine1 AS address_line_1,
        addr.AddressType AS address_type,
        
        -- Phones (repeated)
        ph.PhoneAreaCode AS phone_area_code,
        ph.PhoneNumber AS phone_number,
        ph.PhoneCountryCode AS phone_country_code,
        ph.PhoneType AS phone_type,
        
        -- Other fields from root value record
        CAST(value.Class AS STRING) AS class,
        CAST(value.ShortCode AS STRING) AS short_code,
        CAST(value.OrganisationEntityType AS STRING) AS organisation_entity_type,
        CAST(value.PeriodLockDate AS TIMESTAMP) AS period_lock_date,
        CAST(value.RegistrationNumber AS INT64) AS registration_number,
        CAST(value.EndOfYearLockDate AS TIMESTAMP) AS end_of_year_lock_date,
        CAST(value.DefaultSalesTax AS STRING) AS default_sales_tax,
        CAST(value.FinancialYearEndMonth AS INT64) AS financial_year_end_month,
        CAST(value.SalesTaxPeriod AS STRING) AS sales_tax_period,
        CAST(value.LineOfBusiness AS STRING) AS line_of_business,
        CAST(value.SalesTaxBasis AS STRING) AS sales_tax_basis,
        CAST(value.FinancialYearEndDay AS INT64) AS financial_year_end_day,
        CAST(value.OrganisationID AS STRING) AS organisation_id,
        CAST(value.CreatedDateUTC AS TIMESTAMP) AS created_date_utc,
        CAST(value.TaxNumber AS STRING) AS tax_number,
        CAST(value.EmployerIdentificationNumber AS STRING) AS employer_identification_number,
        CAST(value.OrganisationType AS STRING) AS organisation_type,
        CAST(value.Timezone AS STRING) AS timezone,
        CAST(value.IsDemoCompany AS BOOLEAN) AS is_demo_company,
        
        -- DataFile fields
        CAST(value.DataFile.DataFileCode AS STRING) AS data_file_code,
        CAST(value.DataFile.DataFileName AS STRING) AS data_file_name,
        CAST(value.DataFile.DataFileID AS STRING) AS data_file_id,
        
        -- Legal information fields
        CAST(value.LegalName AS STRING) AS legal_name,
        CAST(value.DefaultPurchasesTax AS STRING) AS default_purchases_tax,
        CAST(value.OrganisationStatus AS STRING) AS organisation_status,
        CAST(value.BaseCurrency AS STRING) AS base_currency,
        CAST(value.APIKey AS STRING) AS api_key,
        CAST(value.CountryCode AS STRING) AS country_code,
        CAST(value.Edition AS STRING) AS edition,
        CAST(value.Version AS STRING) AS version,
        CAST(value.Name AS STRING) AS organisation_name,
        CAST(value.PaysTax AS BOOLEAN) AS pays_tax
    FROM json_base,
    UNNEST(value) AS value,
    UNNEST(value.ExternalLinks) AS el,   -- Unnesting ExternalLinks
    UNNEST(value.Addresses) AS addr,    -- Unnesting Addresses
    UNNEST(value.Phones) AS ph          -- Unnesting Phones
)

SELECT * FROM unflatten_and_cast
QUALIFY ROW_NUMBER() OVER (PARTITION BY organisation_id ORDER BY organisation_name DESC) = 1