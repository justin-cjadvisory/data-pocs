{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:journals"]
  )
}}

WITH json_base AS (
    SELECT *
    FROM {{ ref('base_accounting__journals_accrual') }}
),

unflatten_and_cast AS (
    SELECT 
        CAST(value.SourceType AS STRING) AS source_type,
        CAST(value.Reference AS STRING) AS reference,
        CAST(value.CreatedDateUTC AS TIMESTAMP) AS created_date_utc,
        CAST(value.SourceID AS STRING) AS source_id,
        CAST(value.JournalNumber AS INT64) AS journal_number,
        -- Unnesting JournalLines (repeated)
        jl.JournalLineID AS journal_line_id,
        CAST(jl.TrackingCategories AS ARRAY<STRING>) AS tracking_categories,
        CAST(jl.TaxName AS STRING) AS tax_name,
        CAST(jl.GrossAmount AS FLOAT64) AS gross_amount,
        CAST(jl.Description AS STRING) AS journal_line_description,
        CAST(jl.AccountName AS STRING) AS account_name,
        CAST(jl.NetAmount AS FLOAT64) AS net_amount,
        CAST(jl.AccountType AS STRING) AS account_type,
        CAST(jl.TaxAmount AS FLOAT64) AS tax_amount,
        CAST(jl.AccountCode AS INT64) AS account_code,
        CAST(jl.TaxType AS STRING) AS tax_type,
        CAST(jl.AccountID AS STRING) AS account_id,
        -- DataFile fields
        CAST(value.DataFile.DataFileCode AS STRING) AS data_file_code,
        CAST(value.DataFile.DataFileName AS STRING) AS data_file_name,
        CAST(value.DataFile.DataFileID AS STRING) AS data_file_id,
        -- Journal level fields
        CAST(value.JournalDate AS TIMESTAMP) AS journal_date,
        CAST(value.JournalID AS STRING) AS journal_id
    FROM json_base,
    UNNEST(value) AS value,
    UNNEST(value.JournalLines) AS jl
)

SELECT * FROM unflatten_and_cast

