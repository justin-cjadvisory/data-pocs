{{
  config(
    materialized='table',
    tags=["module:accounting", "submodule:profit_and_loss"]
  )
}}

WITH stg_base AS (
    SELECT *
    FROM  {{ ref('stg_accounting__profit_and_loss_advanced') }}
)

SELECT * FROM stg_base