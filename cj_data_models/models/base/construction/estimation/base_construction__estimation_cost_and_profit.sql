{{
  config(
    materialized='view',
    tags=["module:construction", "submodule:estimation"]
  )
}}

WITH estimation_base AS (
    SELECT *
    FROM  {{ ref('estimation_cost_profit') }}
)

SELECT * FROM estimation_base