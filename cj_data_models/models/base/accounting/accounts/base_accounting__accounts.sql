{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:accounts"]
  )
}}

WITH base AS (
    SELECT *
    FROM {{ source('xero_exports', 'Accounts') }}
)

SELECT * FROM base