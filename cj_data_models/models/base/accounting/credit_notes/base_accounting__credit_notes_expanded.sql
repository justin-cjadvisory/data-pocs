{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:credit_notes"]
  )
}}

WITH base AS (
    SELECT *
    FROM {{ source('xero_exports', 'CreditNotesExpanded') }}
)

SELECT * FROM base