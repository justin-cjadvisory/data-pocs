{{
    config(
        materialized='table',
        tags=["module:construction", "submodule:estimation"]
    )
}}

WITH estimated_pnl_per_build AS (
    SELECT 
        year,
        month,
        from_date,
        to_date,
        build,
        estimate_stage,
        estimate_substage,
        estimate_component,
        resource,
        description,
        supplier,
        cost,
        cost_with_markup,
        profit
    FROM {{ ref('mart_construction__estimation_cost_and_profit') }} 
),

xero_pnl_per_build AS (
    SELECT 
        year,
        month,
        from_date,
        to_date,
        build,
        account_line_type,
        amount
    FROM {{ ref('mart_accounting__profit_and_loss_by_tracking_category') }} 
)

actual_profit_and_cost AS (
    SELECT
        e.year,
        e.month,
        e.from_date,
        e.to_date,
        e.build,
        e.estimate_stage,
        e.estimate_substage,
        e.estimate_component,
        e.resource,
        e.description,
        e.supplier,
        e.cost AS estimated_cost,
        e.cost_with_markup AS estimated_cost_with_markup,
        e.profit AS estimated_profit

    FROM estimated_pnl_per_build e
    LEFT JOIN xero_pnl_per_build x on x.from_date = e.from_date 
        AND x.build = e.build
)

SELECT * FROM actual_profit_and_cost