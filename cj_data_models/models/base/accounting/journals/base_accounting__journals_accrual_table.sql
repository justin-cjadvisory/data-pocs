{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:journals"]
  )
}}

WITH base AS (
    SELECT *
    FROM {{ source('xero_exports', 'JournalsAccrualTable') }}
)

SELECT * FROM base