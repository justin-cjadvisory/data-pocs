{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:accounts"]
  )
}}

WITH base AS (
    SELECT *
    FROM {{ source('xero_exports', 'AccountsTypes') }}
)

SELECT * FROM base