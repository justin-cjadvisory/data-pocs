{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:executive_summary"]
  )
}}

WITH base AS (
    SELECT *
    FROM {{ source('xero_exports', 'ExecutiveSummary') }}
)

SELECT * FROM base