{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:accounts"]
  )
}}

WITH base AS (
    SELECT *
    FROM {{ source('xero_exports', 'AccountsTotals') }}
)

SELECT * FROM base