{{ config(
    materialized='view',
    tags=["module:accounting", "submodule:tax"]
) }}


WITH json_base AS (
    SELECT *
    FROM {{ ref('base_accounting__tax_rates_table') }}  -- Replace with your base model reference
),

unnested_data AS (
    SELECT 
        -- Unnest top-level repeated record
        main.TaxComponentIsCompound AS tax_component_is_compound,
        main.TaxComponentRate AS tax_component_rate,
        main.TaxComponentName AS tax_component_name,
        main.DisplayTaxRate AS display_tax_rate,
        main.TaxComponentIsNonRecoverable AS tax_component_is_non_recoverable,
        main.CanApplyToRevenue AS can_apply_to_revenue,
        main.CanApplyToLiabilities AS can_apply_to_liabilities,
        main.ReportTaxType AS report_tax_type,
        main.CanApplyToEquity AS can_apply_to_equity,
        main.CanApplyToExpenses AS can_apply_to_expenses,
        main.Status AS status,
        main.Name AS tax_name,
        main.EffectiveRate AS effective_rate,
        main.CanApplyToAssets AS can_apply_to_assets,
        main.TaxType AS tax_type,
        main.DataFileID AS data_file_id,
        main.DataFileCode AS data_file_code,
        main.DataFileName AS data_file_name
    FROM json_base,
    UNNEST(value) AS main -- Unnest `value` field
)

SELECT * 
FROM unnested_data
