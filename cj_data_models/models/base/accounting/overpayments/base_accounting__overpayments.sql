{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:overpayments"]
  )
}}

WITH base AS (
    SELECT *
    FROM {{ source('xero_exports', 'Overpayments') }}
)

SELECT * FROM base