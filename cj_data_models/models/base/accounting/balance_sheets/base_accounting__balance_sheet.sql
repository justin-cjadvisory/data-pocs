{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:balance_sheets"]
  )
}}

WITH base AS (
    SELECT *
    FROM {{ source('xero_exports', 'BalanceSheet') }}
)

SELECT * FROM base