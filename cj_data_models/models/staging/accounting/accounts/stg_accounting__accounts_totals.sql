{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:accounts"]
  )
}}

WITH json_base AS (
    SELECT *
    FROM  {{ ref('base_accounting__accounts_totals') }}
),

unflatten_and_cast AS (
    SELECT
        CAST(val.VarianceImpactSign AS INTEGER) AS variance_impact_sign,
        CAST(acc.DrCrSign AS INTEGER) AS dr_cr_sign,
        CAST(acc.AccountType AS STRING) AS account_type,
        CAST(val.Order AS INTEGER) AS acct_order,
        CAST(val.AccountTotal AS STRING) AS account_total,
        CAST(val.IsAccountClassification AS BOOLEAN) AS is_account_classification,
        CAST(val.IsProfitLoss AS BOOLEAN) AS is_profit_loss,
        CAST(val.AccountTotalName AS STRING) AS account_total_name,
        CAST(val.IsBalanceSheet AS BOOLEAN) AS is_balance_sheet
    FROM json_base, 
    UNNEST(value) AS val,  
    UNNEST(val.AccountsTypes) AS acc 
)


SELECT * FROM unflatten_and_cast
