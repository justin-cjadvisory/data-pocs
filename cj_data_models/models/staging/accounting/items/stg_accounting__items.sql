{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:items"]
  )
}}

WITH json_base AS (
    SELECT *
    FROM {{ ref('base_accounting__items') }}
),

unflatten_and_cast AS (
    SELECT
        CAST(value.IsPurchased AS BOOL) AS is_purchased,
        CAST(value.IsSold AS BOOL) AS is_sold,
        CAST(value.QuantityOnHand AS STRING) AS quantity_on_hand,
        CAST(value.TotalCostPool AS STRING) AS total_cost_pool,
        CAST(value.IsTrackedAsInventory AS BOOL) AS is_tracked_as_inventory,
        CAST(value.ItemID AS STRING) AS item_id,
        CAST(value.UpdatedDateUTC AS TIMESTAMP) AS updated_date_utc,
        CAST(value.PurchaseDescription AS STRING) AS purchase_description,
        CAST(value.Description AS STRING) AS description,
        CAST(value.InventoryAssetAccountCode AS STRING) AS inventory_asset_account_code,
        CAST(value.Name AS STRING) AS name,
        CAST(value.Code AS STRING) AS code,
        -- DataFile fields
        CAST(value.DataFile.DataFileCode AS STRING) AS data_file_code,
        CAST(value.DataFile.DataFileName AS STRING) AS data_file_name,
        CAST(value.DataFile.DataFileID AS STRING) AS data_file_id,
        -- PurchaseDetails fields
        CAST(value.PurchaseDetails.TaxType AS STRING) AS purchase_details_tax_type,
        CAST(value.PurchaseDetails.COGSAccountCode AS STRING) AS purchase_details_cogs_account_code,
        CAST(value.PurchaseDetails.AccountCode AS STRING) AS purchase_details_account_code,
        CAST(value.PurchaseDetails.UnitPrice AS STRING) AS purchase_details_unit_price,
        -- SalesDetails fields
        CAST(value.SalesDetails.TaxType AS STRING) AS sales_details_tax_type,
        CAST(value.SalesDetails.AccountCode AS STRING) AS sales_details_account_code,
        CAST(value.SalesDetails.UnitPrice AS INT64) AS sales_details_unit_price
    FROM json_base,
    UNNEST(value) AS value
)

SELECT * FROM unflatten_and_cast