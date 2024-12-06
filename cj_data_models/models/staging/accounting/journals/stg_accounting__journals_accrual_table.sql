{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:journals"]
  )
}}

WITH json_base AS (
    SELECT *
    FROM {{ ref('base_accounting__journals_accrual_table') }} 
),

unflatten_and_cast AS (
    SELECT 
        CAST(value.CreatedDateUTC AS TIMESTAMP) AS created_date_utc,
        CAST(value.TrackingOption2Name AS STRING) AS tracking_option_2_name,
        CAST(value.TrackingOption2ID AS STRING) AS tracking_option_2_id,
        CAST(value.TrackingCategory2Name AS STRING) AS tracking_category_2_name,
        CAST(value.TrackingCategory2ID AS STRING) AS tracking_category_2_id,
        CAST(value.TrackingOption1ID AS STRING) AS tracking_option_1_id,
        CAST(value.TrackingCategory1Name AS STRING) AS tracking_category_1_name,
        CAST(value.TrackingOption1Name AS STRING) AS tracking_option_1_name,
        CAST(value.TaxName AS STRING) AS tax_name,
        CAST(value.TaxType AS STRING) AS tax_type,
        CAST(value.GrossAmount AS FLOAT64) AS gross_amount,
        CAST(value.Description AS STRING) AS description,
        CAST(value.AccountName AS STRING) AS account_name,
        CAST(value.AccountType AS STRING) AS account_type,
        CAST(value.AccountCode AS INT64) AS account_code,
        CAST(value.TrackingCategory1ID AS STRING) AS tracking_category_1_id,
        CAST(value.DataFileID AS STRING) AS data_file_id,
        CAST(value.DataFileCode AS STRING) AS data_file_code,
        CAST(value.AccountID AS STRING) AS account_id,
        CAST(value.JournalID AS STRING) AS journal_id,
        CAST(value.Reference AS STRING) AS reference,
        CAST(value.SourceID AS STRING) AS source_id,
        CAST(value.SourceType AS STRING) AS source_type,
        CAST(value.NetAmount AS FLOAT64) AS net_amount,
        CAST(value.JournalNumber AS INT64) AS journal_number,
        CAST(value.DataFileName AS STRING) AS data_file_name,
        CAST(value.JournalDate AS DATE) AS journal_date,
        CAST(value.TaxAmount AS FLOAT64) AS tax_amount,
        CAST(value.JournalLineID AS STRING) AS journal_line_id
    FROM json_base,
    UNNEST(value) AS value
)

SELECT * FROM unflatten_and_cast
QUALIFY ROW_NUMBER() OVER (PARTITION BY updated_date_utc, journal_line_id ORDER BY updated_date_utc DESC) = 1