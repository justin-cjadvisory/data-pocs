{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:tracking_categories"]
  )
}}

WITH base AS (
    SELECT *
    FROM {{ source('xero_exports', 'TrackingCategories') }}
)

SELECT * FROM base