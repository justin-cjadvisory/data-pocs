{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:balance_sheets"]
  )
}}

WITH base AS (
    SELECT *
    FROM {{ source('xero_exports', 'BalanceSheetAdvanced') }}
)

SELECT * FROM base