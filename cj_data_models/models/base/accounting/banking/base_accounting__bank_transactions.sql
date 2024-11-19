{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:banking"]
  )
}}

WITH base AS (
    SELECT *
    FROM {{ source('xero_exports', 'BankTransactions') }}
)

SELECT * FROM base