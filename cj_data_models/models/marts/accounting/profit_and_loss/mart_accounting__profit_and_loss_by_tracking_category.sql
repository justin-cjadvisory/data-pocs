{{
  config(
    materialized='table',
    tags=["module:accounting", "submodule:profit_and_loss"]
  )
}}

WITH stg_base AS (
    SELECT *
    FROM  {{ ref('stg_accounting__profit_and_loss_by_tracking_category') }}
),

load_historical_data AS (
    SELECT *
    FROM {{ ref('stg_accounting__profit_and_loss_by_tracking_category_historical') }}
),

populate_data AS (
    SELECT
      EXTRACT(YEAR FROM h.from_date) AS year,
      EXTRACT(MONTH FROM h.from_date) AS month,
      h.from_date,
      h.to_date,
      h.build,
      h.account_line_type,
      h.account_name,
      h.amount
    FROM load_historical_data h
    UNION ALL
    SELECT
      EXTRACT(YEAR FROM s.from_date) AS year,
      EXTRACT(MONTH FROM s.from_date) AS month,
      s.from_date,
      s.to_date,
      s.build,
      s.account_line_type,
      s.account_name,
      s.amount
    FROM stg_base s
)

SELECT * FROM populate_data