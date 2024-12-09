{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:users"]
  )
}}

WITH base AS (
    SELECT *
    FROM {{ source('xero_exports', 'Users') }}
)

SELECT * FROM base