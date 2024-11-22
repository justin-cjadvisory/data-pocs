{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:accounts"]
  )
}}

WITH json_base AS (
    SELECT *
    FROM  {{ ref('base_accounting__accounts_types') }}
),

unflatten_and_cast AS (
    SELECT
        CAST(val.VarianceImpactSign AS INTEGER) AS variance_impact_sign,
        CAST(val.DrCrSign AS INTEGER) AS dr_cr_sign,
        CAST(val.IsCredit AS BOOLEAN) AS is_credit,
        CAST(val.AccountTypeName AS STRING) AS account_type_name,
        CAST(val.IsBalanceSheet AS BOOLEAN) AS is_balance_sheet,
        CAST(val.IsProfitLoss AS BOOLEAN) AS is_profit_loss,
        CAST(val.Class AS STRING) AS class,
        CAST(val.IsDebit AS BOOLEAN) AS is_debit,
        CAST(val.AccountType AS STRING) AS account_type
    FROM json_base, 
    UNNEST(value) AS val  
)

SELECT * FROM unflatten_and_cast
