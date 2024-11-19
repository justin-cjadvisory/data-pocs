{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:profit_and_loss"]
  )
}}

WITH base AS (
    SELECT *
    FROM {{ source('xero_exports', 'ProfitAndLossMultiPeriodTable') }}
)

SELECT * FROM base