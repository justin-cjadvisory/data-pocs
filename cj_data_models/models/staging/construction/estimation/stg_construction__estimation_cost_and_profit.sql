{{
  config(
    materialized='view',
    tags=["module:construction", "submodule:estimation"]
  )
}}

WITH estimation_base AS (
    SELECT *
    FROM  {{ ref('base_construction__estimation_cost_and_profit') }}
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
        CAST(project AS STRING) AS build, 
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
),

add_more_dates AS (
    SELECT
        c.*,
        EXTRACT(YEAR FROM quoted_date) AS year,
        EXTRACT(MONTH FROM quoted_date) AS month,
        DATE_TRUNC(quoted_date, MONTH) AS from_date,
        LAST_DAY(quoted_date) AS to_date
    FROM convert_appropriate_datatype c
),

rename_build AS (
    SELECT
        a.* EXCEPT (build),
        CASE
            WHEN build = '2 HAWDON ST. EAGLEMONT' THEN '2 HAWDON STREET EAGLEMONT'
            WHEN build = '10-14 VIRGINIA COURT, MONTMORENCY' THEN '10-14 VIRGINIA COURT'
            WHEN build = '4 BREEN ST PRESTON' THEN '4 BREEN STREET PRESTON'
            WHEN build = '9A QUINNS RD WELLSFORD' THEN '9A QUINNS RD WELLSFORD VIC 3551'
            WHEN build = '18 BOYCE AVE. BRIAR HILL' THEN '18 BOYCE AVENUE BRIAR HILL'
            WHEN build = '10 SHEFFIELD ST ELTHAM' THEN '10 SHEFFIELD ST ELTHAM'
            WHEN build = '28 MORTIMER ST HEIDLEBERG' THEN '28 MORTIMER STREET HEIDELBERG'
            WHEN build = '68 LUCK ST ELTHAM' THEN '68 LUCK STREET ELTHAM'
            ELSE build
        END AS build
    FROM add_more_dates a
)

SELECT * FROM rename_build
