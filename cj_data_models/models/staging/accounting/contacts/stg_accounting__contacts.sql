{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:contacts"]
  )
}}

WITH json_base AS (
    SELECT *
    FROM {{ ref('base_accounting__contacts') }}
), 

unflatten_and_cast AS (
    SELECT
        CAST(value.HasAttachments AS BOOLEAN) AS has_attachments,
        CAST(value.IsSupplier AS BOOLEAN) AS is_supplier,
        CAST(value.DefaultCurrency AS STRING) AS default_currency,
        CAST(value.IsCustomer AS BOOLEAN) AS is_customer,
        CAST(value.UpdatedDateUTC AS TIMESTAMP) AS updated_date_utc,
        
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

        ARRAY_TO_STRING(
            ARRAY(
                SELECT CONCAT(
                    address.Country, ', ', 
                    address.PostalCode, ', ', 
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

        CAST(value.Website AS STRING) AS website,
        CAST(value.TaxNumber AS STRING) AS tax_number,

        CAST(value.Balances.AccountsPayable.Overdue AS FLOAT64) AS overdue_accounts_payable,
        CAST(value.Balances.AccountsPayable.Outstanding AS FLOAT64) AS outstanding_accounts_payable,
        CAST(value.Balances.AccountsReceivable.Overdue AS FLOAT64) AS overdue_accounts_receivable,
        CAST(value.Balances.AccountsReceivable.Outstanding AS FLOAT64) AS outstanding_accounts_receivable,

        CAST(value.DataFile.DataFileCode AS STRING) AS data_file_code,
        CAST(value.DataFile.DataFileName AS STRING) AS data_file_name,
        CAST(value.DataFile.DataFileID AS STRING) AS data_file_id,

        CAST(value.SkypeUserName AS STRING) AS skype_username,
        CAST(value.EmailAddress AS STRING) AS email_address,
        CAST(value.LastName AS STRING) AS last_name,
        CAST(value.FirstName AS STRING) AS first_name,
        CAST(value.ContactStatus AS STRING) AS contact_status,
        CAST(value.ContactID AS STRING) AS contact_id,
        CAST(value.Name AS STRING) AS name,
        CAST(value.AccountNumber AS STRING) AS account_number,
        CAST(value.CompanyNumber AS STRING) AS company_number,
        CAST(value.ContactNumber AS STRING) AS contact_number,
        CAST(value.BankAccountDetails AS STRING) AS bank_account_details
    FROM json_base,
    UNNEST(value) AS value  
)

SELECT * FROM unflatten_and_cast
