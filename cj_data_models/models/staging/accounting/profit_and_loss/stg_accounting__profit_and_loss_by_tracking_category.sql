{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:profit_and_loss"]
  )
}}

WITH json_base AS (
    SELECT *
    FROM  {{ ref('base_accounting__profit_and_loss_by_tracking_category') }}
),

unflatten_and_cast AS (
    SELECT
        CAST(val.TrackingCategoryID2 AS STRING) AS tracking_category_id2,
        CAST(val.UpdatedDateUTC AS TIMESTAMP) AS updated_date_utc,
        CAST(val.PaymentsOnly AS BOOLEAN) AS payments_only,
        CAST(val.StandardLayout AS BOOLEAN) AS standard_layout,
        CAST(val.ToDate AS DATE) AS to_date,
        CAST(val.FromDate AS DATE) AS from_date,
        CAST(val.TrackingCategoryID AS STRING) AS tracking_category_id,
        CAST(amount.Amount AS FLOAT64) AS amount,
        CAST(amount.TrackingOption2.Option AS STRING) AS tracking_option_2,
        CAST(line.AccountID AS STRING) AS account_id,
        CAST(line.AccountName AS STRING) AS account_name,
        CAST(line.AccountLineType AS STRING) AS account_line_type,
        CAST(line.LineType AS STRING) AS line_type,
        UPPER(CAST(amount.TrackingOption.Option AS STRING)) AS build,
        CAST(val.DataFile.DataFileCode AS STRING) AS datafile_code,
        CAST(val.DataFile.DataFileName AS STRING) AS datafile_name,
        CAST(val.DataFile.DataFileID AS STRING) AS datafile_id
    FROM
        json_base,
        UNNEST(value) AS val,               
        UNNEST(val.Lines) AS line,          
        UNNEST(line.Amounts) AS amount   
        QUALIFY ROW_NUMBER() OVER (PARTITION BY from_date, build, account_name ORDER BY from_date DESC) = 1
),

fix_9a_quinns_rd AS (
    SELECT * EXCEPT (build),
      CASE 
        WHEN build = '(A QUINNS RD WELLSFORD' THEN '9A QUINNS RD WELLSFORD VIC 3551'
      ELSE build
      END AS build
    FROM unflatten_and_cast
)

SELECT * FROM fix_9a_quinns_rd
WHERE build != 'TOTAL'
and line_type != 'Total'
