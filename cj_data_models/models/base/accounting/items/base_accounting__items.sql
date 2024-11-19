{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:items"]
  )
}}

WITH base AS (
    SELECT *
    FROM {{ source('xero_exports', 'Items') }}
)

SELECT * FROM base