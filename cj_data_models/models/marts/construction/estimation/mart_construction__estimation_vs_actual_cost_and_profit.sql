{{
    config(
        materialized='table',
        tags=["module:construction", "submodule:estimation"]
    )
}}

WITH estimated_pnl_per_build AS (
    SELECT 
        build,
        cost,
        cost_with_markup,
        profit
    FROM {{ ref('mart_construction__estimation_cost_and_profit') }} 
),

xero_pnl_per_build AS (
    SELECT 
        build,
        account_line_type,
        amount
    FROM {{ ref('mart_accounting__profit_and_loss_by_tracking_category') }} 
),

actual_profit_and_cost AS (
    SELECT
        e.build,
        SUM(e.cost_with_markup) AS estimated_cost_with_markup,
        SUM(e.profit) AS estimated_profit,
        SUM(IF(x.account_line_type IN ('Less Operating Expenses', 'Less Cost of Sales'), amount, 0)) AS actual_cost,
        SUM(IF(x.account_line_type IN ('Income'), amount, 0)) AS actual_income
    FROM estimated_pnl_per_build e
    LEFT JOIN xero_pnl_per_build x on x.build = e.build 
    GROUP BY ALL
),

compute_actual_profit AS (
    SELECT
        ap.*,
        SUM(actual_income) - SUM(actual_cost) AS actual_profit
    FROM actual_profit_and_cost ap
    GROUP BY ALL
),

create_flags AS (
    SELECT 
        c.*,
        CASE 
            WHEN actual_cost > estimated_cost_with_markup THEN TRUE
            ELSE FALSE
        END AS exceeded_estimated_cost,
        CASE 
            WHEN actual_profit > estimated_profit THEN TRUE
            ELSE FALSE
        END AS exceeded_estimated_profit
    FROM compute_actual_profit c
)

SELECT * FROM create_flags
WHERE (actual_cost > 0 AND actual_income > 0)
