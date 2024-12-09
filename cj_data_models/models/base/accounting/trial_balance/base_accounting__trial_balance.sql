{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:trial_balance"]
  )
}}

WITH base AS (
    SELECT *
    FROM {{ source('xero_exports', 'TrialBalance') }}
)

SELECT * FROM base