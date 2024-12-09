{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:tax"]
  )
}}

WITH base AS (
    SELECT *
    FROM {{ source('xero_exports', 'TaxRatesTable') }}
)

SELECT * FROM base