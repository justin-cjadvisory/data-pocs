{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:contacts"]
  )
}}

WITH base AS (
    SELECT *
    FROM {{ source('xero_exports', 'Contacts') }}
)

SELECT * FROM base