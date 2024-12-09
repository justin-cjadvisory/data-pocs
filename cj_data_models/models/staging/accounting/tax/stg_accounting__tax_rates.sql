{{ config(
    materialized='view',
    tags=["module:accounting", "submodule:tax"]
) }}

WITH json_base AS (
    SELECT *
    FROM {{ ref('base_accounting__tax_rates') }}  -- Replace with your base model reference
),

unnested_data AS (
    SELECT 
        -- Flatten the top-level repeated record
        tax_component_item.IsCompound AS is_compound,
        tax_component_item.IsNonRecoverable AS is_non_recoverable,
        tax_component_item.Rate AS tax_rate,
        tax_component_item.Name AS tax_name,
        main.Status AS status,
        main.CanApplyToAssets AS can_apply_to_assets,
        main.EffectiveRate AS effective_rate,
        main.CanApplyToLiabilities AS can_apply_to_liabilities,
        main.ReportTaxType AS report_tax_type,
        main.CanApplyToEquity AS can_apply_to_equity,
        main.DisplayTaxRate AS display_tax_rate,
        main.CanApplyToRevenue AS can_apply_to_revenue,
        main.TaxType AS tax_type,
        main.CanApplyToExpenses AS can_apply_to_expenses,
        main.Name AS tax_name_overall,
        main.DataFile.DataFileCode AS datafile_code,
        main.DataFile.DataFileName AS datafile_name,
        main.DataFile.DataFileID AS datafile_id
    FROM json_base,
    UNNEST(json_base.value) AS main,            -- Unnest the `value` repeated record
    UNNEST(main.TaxComponents) AS tax_component_item -- Further unnest `TaxComponents`
    QUALIFY ROW_NUMBER() OVER (PARTITION BY tax_name ORDER BY tax_name DESC) = 1
)

SELECT * 
FROM unnested_data
