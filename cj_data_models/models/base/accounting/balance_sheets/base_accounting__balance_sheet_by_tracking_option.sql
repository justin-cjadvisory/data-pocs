{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:balance_sheets"]
  )
}}

WITH base AS (
    SELECT *
    FROM {{ source('xero_exports', 'BalanceSheetByTrackingOption') }}
)

SELECT * FROM base