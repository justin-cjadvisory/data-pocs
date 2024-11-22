{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:profit_and_loss"]
  )
}}

WITH json_base AS (
    SELECT *
    FROM  {{ ref('base_accounting__profit_and_loss_table') }}
),

unflatten_and_cast AS (
    SELECT
        CAST(val.Amount AS FLOAT64) AS amount,
        CAST(val.AccountLineType AS STRING) AS account_line_type,
        CAST(val.FromDate AS DATE) AS from_date,
        CAST(val.ToDate AS DATE) AS to_date,
        CAST(val.AccountName AS STRING) AS account_name,
        CAST(val.AccountID AS STRING) AS account_id,
        CAST(val.LineType AS STRING) AS line_type,
        CAST(val.StandardLayout AS BOOLEAN) AS standard_layout,
        CAST(val.PaymentsOnly AS BOOLEAN) AS payments_only,
        CAST(val.DataFileCode AS STRING) AS datafile_code,
        CAST(val.DataFileName AS STRING) AS datafile_name,
        CAST(val.DataFileID AS STRING) AS datafile_id
    FROM
        json_base,
        UNNEST(value) AS val  
)

SELECT * FROM unflatten_and_cast
