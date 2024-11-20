{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:accounts"]
  )
}}

WITH json_base AS (
    SELECT *
    FROM  {{ ref('base_accounting__accounts_class') }}
),

unflatten_and_cast AS (
    SELECT
        CAST(val.VarianceImpactSign AS INT64) AS variance_impact_sign,
        CAST(val.DrCrSign AS INT64) AS dr_cr_sign,
        CAST(val.IsCredit AS BOOLEAN) AS is_credit,
        CAST(val.IsBalanceSheet AS BOOLEAN) AS is_balance_sheet,
        CAST(val.IsDebit AS BOOLEAN) AS is_debit,
        CAST(val.IsProfitLoss AS BOOLEAN) AS is_profit_loss,
        CAST(val.ProfitLossSign AS INT64) AS profit_loss_sign,
        CAST(val.Class AS STRING) AS class
    FROM json_base, 
    UNNEST(value) AS val
)


SELECT * FROM unflatten_and_cast
