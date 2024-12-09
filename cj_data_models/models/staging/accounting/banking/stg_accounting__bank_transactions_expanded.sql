{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:banking"]
  )
}}

WITH json_base AS (
    SELECT *
    FROM {{ ref('base_accounting__bank_transactions_expanded') }}
),

unflatten_and_cast AS (
    SELECT
        CAST(value.CurrencyCode AS STRING) AS currency_code,
        CAST(value.LineAmountTypes AS STRING) AS line_amount_types,
        CAST(value.UpdatedDateUTC AS TIMESTAMP) AS updated_date_utc,
        CAST(value.Date AS TIMESTAMP) AS date,
        CAST(value.Contact.Name AS STRING) AS contact_name,
        CAST(value.Contact.ContactID AS STRING) AS contact_id,
        CAST(value.OverpaymentID AS STRING) AS overpayment_id,
        CAST(value.Type AS STRING) AS type,
        CAST(value.PrepaymentID AS STRING) AS prepayment_id,
        CAST(value.CurrencyRate AS INT64) AS currency_rate,
        CAST(value.IsReconciled AS BOOLEAN) AS is_reconciled,
        CAST(value.Total AS FLOAT64) AS total,
        CAST(value.BankAccount.Code AS STRING) AS bank_account_code,
        CAST(value.BankAccount.Name AS STRING) AS bank_account_name,
        CAST(value.BankAccount.AccountID AS STRING) AS bank_account_id,
        CAST(value.BatchPayment AS STRING) AS batch_payment,
        CAST(value.Reference AS STRING) AS reference,
        CAST(value.HasAttachments AS BOOLEAN) AS has_attachments,
        CAST(value.TotalTax AS FLOAT64) AS total_tax,
        
        -- Unnest the LineItems array
        li.LineItemID AS line_item_id,
        
        -- Aggregate the Tracking array into a single string
        ARRAY_TO_STRING(li.Tracking, ', ') AS tracking,  -- Joining with a comma as delimiter
        
        CAST(li.AccountID AS STRING) AS account_id,
        CAST(li.LineAmount AS FLOAT64) AS line_amount,
        CAST(li.TaxType AS STRING) AS tax_type,
        CAST(li.AccountCode AS INT64) AS account_code,
        CAST(li.TaxAmount AS FLOAT64) AS tax_amount,
        CAST(li.UnitAmount AS FLOAT64) AS unit_amount,
        CAST(li.Quantity AS INT64) AS quantity,
        CAST(li.Description AS STRING) AS description,
        CAST(li.ItemCode AS STRING) AS item_code,
        CAST(value.Url AS STRING) AS line_item_url,
        CAST(value.Status AS STRING) AS line_item_status,
        CAST(value.BankTransactionID AS STRING) AS line_item_bank_transaction_id,
        CAST(value.SubTotal AS FLOAT64) AS line_item_sub_total,
        
        -- Unnest the DataFile struct
        CAST(value.DataFile.DataFileCode AS STRING) AS data_file_code,
        CAST(value.DataFile.DataFileName AS STRING) AS data_file_name,
        CAST(value.DataFile.DataFileID AS STRING) AS data_file_id
    FROM json_base,
    UNNEST(value) AS value,  -- Unnest the root array
    UNNEST(value.LineItems) AS li  -- Unnest the LineItems array
    QUALIFY ROW_NUMBER() OVER (PARTITION BY updated_date_utc, account_id ORDER BY updated_date_utc DESC) = 1
)


SELECT * FROM unflatten_and_cast
