{{
  config(
    materialized='view',
    tags=["module:construction"]
  )
}}

WITH estimation_base AS (
    SELECT *
    FROM  {{ ref('estimation_cost_profit') }}
),

remove_na AS (
    SELECT * EXCEPT(quoted_date),
        CASE 
            WHEN quoted_date = 'N/A' THEN NULL
            ELSE quoted_date
        END AS quoted_date
    FROM estimation_base
),

convert_appropriate_datatype AS (
    SELECT
        PARSE_DATE('%d/%m/%Y', quoted_date) AS quoted_date,
        CAST(project AS STRING) AS project, 
        CAST(estimate_stage AS STRING) AS estimate_stage, 
        CAST(estimate_substage AS STRING) AS estimate_substage, 
        CAST(estimate_component AS STRING) AS estimate_component, 
        CAST(resource AS STRING) AS resource, 
        CAST(short_code AS STRING) AS short_code, 
        CAST(description AS STRING) AS description,
        CASE
            WHEN REGEXP_CONTAINS(quantity, r'\d+/\d+') THEN
            ROUND(SAFE_CAST(SPLIT(quantity, '/')[OFFSET(0)] AS FLOAT64) / SAFE_CAST(SPLIT(quantity, '/')[OFFSET(1)] AS FLOAT64), 2)
        ELSE ROUND(SAFE_CAST(quantity AS INT64), 2)
        END AS quantity,
        CAST(units AS STRING) AS units,
        CAST(supplier AS STRING) AS supplier,
        ROUND(SAFE_CAST(REPLACE(REPLACE(unit_cost, '$', ''), ',', '') AS FLOAT64), 2) AS unit_cost,
        ROUND(SAFE_CAST(REPLACE(REPLACE(cost, '$', ''), ',', '') AS FLOAT64), 2) AS cost,
        ROUND(SAFE_CAST(markup AS FLOAT64), 2) AS markup,
        ROUND(SAFE_CAST(REPLACE(REPLACE(cost_with_markup, '$', ''), ',', '') AS FLOAT64), 2) AS cost_with_markup,
        ROUND(SAFE_CAST(profit AS FLOAT64), 2) AS profit,
    FROM remove_na
)

SELECT * FROM convert_appropriate_datatype
