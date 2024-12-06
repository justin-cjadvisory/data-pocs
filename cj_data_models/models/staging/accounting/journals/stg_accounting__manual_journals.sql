{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:journals"]
  )
}}

WITH json_base AS (
    SELECT *
    FROM {{ ref('base_accounting__manual_journals') }}  -- Replace with your base table
),

unflatten_and_cast AS (
    SELECT 
        CAST(value.Url AS STRING) AS url,
        CAST(value.HasAttachments AS BOOLEAN) AS has_attachments,
        CAST(value.Narration AS STRING) AS narration,
        CAST(value.UpdatedDateUTC AS TIMESTAMP) AS updated_date_utc,
        CAST(value.ShowOnCashBasisReports AS BOOLEAN) AS show_on_cash_basis_reports,
        CAST(value.ManualJournalID AS STRING) AS manual_journal_id,
        CAST(value.Status AS STRING) AS status,
        CAST(value.Date AS TIMESTAMP) AS date,
        
        -- DataFile fields
        CAST(value.DataFile.DataFileCode AS STRING) AS data_file_code,
        CAST(value.DataFile.DataFileName AS STRING) AS data_file_name,
        CAST(value.DataFile.DataFileID AS STRING) AS data_file_id
    FROM json_base,
    UNNEST(value) AS value
)

SELECT * FROM unflatten_and_cast
QUALIFY ROW_NUMBER() OVER (PARTITION BY updated_date_utc, manual_journal_id ORDER BY manual_journal_id DESC) = 1