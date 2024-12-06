{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:journals"]
  )
}}

WITH json_base AS (
    SELECT *
    FROM {{ ref('base_accounting__manual_journals_expanded') }}  -- Replace with your base table
),

unflatten_and_cast AS (
    SELECT 
        CAST(value.HasAttachments AS BOOLEAN) AS has_attachments,
        CAST(value.Url AS STRING) AS url,
        
        -- Unnesting JournalLines (repeated)
        jl.AccountID AS account_id,
        CAST(jl.TaxType AS STRING) AS tax_type,
        CAST(jl.LineAmount AS FLOAT64) AS line_amount,
        CAST(jl.TaxAmount AS FLOAT64) AS tax_amount,
        CAST(jl.AccountCode AS INT64) AS account_code,
        CAST(jl.Description AS STRING) AS journal_line_description,
        
        -- Unnesting Tracking within JournalLines
        t.TrackingOptionID AS tracking_option_id,
        t.TrackingCategoryID AS tracking_category_id,
        t.Option AS tracking_option,
        t.Name AS tracking_name,
        
        -- Other fields from the root value record
        CAST(value.Narration AS STRING) AS narration,
        CAST(value.ShowOnCashBasisReports AS BOOLEAN) AS show_on_cash_basis_reports,
        CAST(value.ManualJournalID AS STRING) AS manual_journal_id,
        CAST(value.UpdatedDateUTC AS TIMESTAMP) AS updated_date_utc,
        CAST(value.LineAmountTypes AS STRING) AS line_amount_types,
        CAST(value.Status AS STRING) AS status,
        CAST(value.Date AS TIMESTAMP) AS date,
        
        -- DataFile fields
        CAST(value.DataFile.DataFileCode AS STRING) AS data_file_code,
        CAST(value.DataFile.DataFileName AS STRING) AS data_file_name,
        CAST(value.DataFile.DataFileID AS STRING) AS data_file_id
    FROM json_base,
    UNNEST(value) AS value,
    UNNEST(value.JournalLines) AS jl,
    UNNEST(jl.Tracking) AS t  -- Unnesting Tracking within JournalLines
)

SELECT * FROM unflatten_and_cast
QUALIFY ROW_NUMBER() OVER (PARTITION BY updated_date_utc, account_id ORDER BY updated_date_utc DESC) = 1