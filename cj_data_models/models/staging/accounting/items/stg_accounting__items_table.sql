{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:items"]
  )
}}

WITH json_base AS (
    SELECT *
    FROM {{ ref('base_accounting__items_table') }}
),

unflatten_and_cast AS (
    SELECT
        CAST(value.UpdatedDateUTC AS TIMESTAMP) AS updated_date_utc,
        CAST(value.QuantityOnHand AS STRING) AS quantity_on_hand,
        CAST(value.TotalCostPool AS STRING) AS total_cost_pool,
        CAST(value.IsTrackedAsInventory AS BOOL) AS is_tracked_as_inventory,
        CAST(value.SalesAccountCode AS STRING) AS sales_account_code,
        CAST(value.InventoryAssetAccountCode AS STRING) AS inventory_asset_account_code,
        CAST(value.PurchaseTaxType AS STRING) AS purchase_tax_type,
        CAST(value.PurchaseDescription AS STRING) AS purchase_description,
        CAST(value.SalesTaxType AS STRING) AS sales_tax_type,
        CAST(value.Description AS STRING) AS description,
        CAST(value.IsSold AS BOOL) AS is_sold,
        CAST(value.SalesUnitPrice AS INT64) AS sales_unit_price,
        CAST(value.IsPurchased AS BOOL) AS is_purchased,
        CAST(value.Name AS STRING) AS name,
        CAST(value.COGSAccountCode AS STRING) AS cogs_account_code,
        CAST(value.PurchaseUnitPrice AS STRING) AS purchase_unit_price,
        CAST(value.ItemID AS STRING) AS item_id,
        CAST(value.PurchaseAccountCode AS STRING) AS purchase_account_code,
        CAST(value.Code AS STRING) AS code,
        CAST(value.DataFileID AS STRING) AS data_file_id,
        CAST(value.DataFileCode AS STRING) AS data_file_code,
        CAST(value.DataFileName AS STRING) AS data_file_name
    FROM json_base,
    UNNEST(value) AS value
)

SELECT * FROM unflatten_and_cast