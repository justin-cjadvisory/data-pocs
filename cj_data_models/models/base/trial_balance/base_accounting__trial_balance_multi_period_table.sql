{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:trial_balance"]
  )
}}

WITH base AS (
    SELECT *
    FROM {{ source('xero_exports', 'TrialBalanceMultiPeriodTable') }}
)

SELECT * FROM base