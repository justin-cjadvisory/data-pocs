{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:tax"]
  )
}}

WITH base AS (
    SELECT *
    FROM {{ source('xero_exports', 'TaxRates') }}
)

SELECT * FROM base