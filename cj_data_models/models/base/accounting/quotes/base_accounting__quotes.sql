{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:quotes"]
  )
}}

WITH base AS (
    SELECT *
    FROM {{ source('xero_exports', 'Quotes') }}
)

SELECT * FROM base