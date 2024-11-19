{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:payments"]
  )
}}

WITH base AS (
    SELECT *
    FROM {{ source('xero_exports', 'PaymentsTable') }}
)

SELECT * FROM base