{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:journals"]
  )
}}

WITH base AS (
    SELECT *
    FROM {{ source('xero_exports', 'ManualJournals') }}
)

SELECT * FROM base