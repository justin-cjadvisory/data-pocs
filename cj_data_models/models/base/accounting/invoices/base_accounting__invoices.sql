{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:invoices"]
  )
}}

WITH base AS (
    SELECT *
    FROM {{ source('xero_exports', 'Invoices') }}
)

SELECT * FROM base