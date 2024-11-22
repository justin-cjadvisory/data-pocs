{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:profit_and_loss"]
  )
}}

WITH json_base AS (
    SELECT *
    FROM  {{ ref('base_accounting__profit_and_loss') }}
),

unflatten_and_cast AS (
    SELECT
        CAST(line.Amount AS FLOAT64) AS amount,
        CAST(line.AccountName AS STRING) AS account_name,
        CAST(line.AccountID AS STRING) AS account_id,
        CAST(line.AccountLineType AS STRING) AS account_line_type,
        CAST(line.LineType AS STRING) AS line_type,
        CAST(val.UpdatedDateUTC AS TIMESTAMP) AS updated_date_utc,
        CAST(val.PaymentsOnly AS BOOLEAN) AS payments_only,
        CAST(val.StandardLayout AS BOOLEAN) AS standard_layout,
        CAST(val.ToDate AS DATE) AS to_date,
        CAST(val.FromDate AS DATE) AS from_date,
        CAST(val.DataFile.DataFileCode AS STRING) AS datafile_code,
        CAST(val.DataFile.DataFileName AS STRING) AS datafile_name,
        CAST(val.DataFile.DataFileID AS STRING) AS datafile_id
    FROM json_base,
    UNNEST(value) AS val,
    UNNEST(val.Lines) AS line
)

SELECT * FROM unflatten_and_cast
