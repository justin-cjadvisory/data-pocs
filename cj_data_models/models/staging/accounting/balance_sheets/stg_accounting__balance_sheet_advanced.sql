{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:balance_sheets"]
  )
}}

WITH json_base AS (
    SELECT *
    FROM  {{ ref('base_accounting__balance_sheet_advanced') }}
),

unflatten_and_cast AS (
    SELECT
        val.AccountLineType  -- Accessing the field directly
    FROM json_base,
    UNNEST(value) AS val  -- Unnest the array of structs in 'value'
)

SELECT * FROM unflatten_and_cast
