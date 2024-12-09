{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:profit_and_loss"]
  )
}}

WITH estimation_base AS (
    SELECT *
    FROM  {{ ref('consolidated_data') }}
)

SELECT * FROM estimation_base