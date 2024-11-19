{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:organisation"]
  )
}}

WITH base AS (
    SELECT *
    FROM {{ source('xero_exports', 'Organisation') }}
)

SELECT * FROM base