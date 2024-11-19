{{
    config(
        materialized='table',
        tags=["module:construction", "submodule:estimation"]
    )
}}

WITH stg_base AS (
    SELECT * 
    FROM {{ ref('stg_construction__estimation_cost_and_profit') }}
)

SELECT * FROM stg_base
WHERE project IS NOT NULL