{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:balance_sheets"]
  )
}}

WITH base AS (
    SELECT *
    FROM {{ source('xero_exports', 'BalanceSheetMultiPeriodTable') }}
)

SELECT * FROM base