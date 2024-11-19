{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:contacts"]
  )
}}

WITH base AS (
    SELECT *
    FROM {{ source('xero_exports', 'ContactsExpanded') }}
)

SELECT * FROM base