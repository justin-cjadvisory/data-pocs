{{
  config(
    materialized='view',
    tags=["module:accounting", "submodule:profit_and_loss"]
  )
}}

WITH json_base AS (
    SELECT *
    FROM  {{ ref('base_accounting__profit_and_loss_by_tracking_category_historical') }}
),

correct_account_line_type AS (
    SELECT * EXCEPT(account_line_type),
      CASE
        WHEN Account IN ('License & Permits', 'Wages and Salaries', 'Insurance - Warranty', 'Superannuation') THEN 'Cost of Sales'
        WHEN Account IN (
          'Insurance - Business', 'Telephone & Internet', 'Protective Clothing', 'Advertising', 
          'Repairs and Maintenance', 'Light, Power, Heating', 'Client Meetings', 'Interest Expense',
          'Office Expenses', 'Staff Education/Training', 'Entertainment', 'Subscriptions', 'Hire Equipment',
          'Consulting & Accounting', 'Staff Amenities', 'Workcover Expenses', 'Bank Fees', 'Motor Vehicle Expenses',
          'Travel - National', 'Depreciation', 'Filing fees', 'Horse', 'Wage - Director', 'Lease repayments', 'Borrowing Expenses',
          'Fines', 'Donations', 'Income Tax Expense', 'Legal expenses', 'Payroll Tax'
        ) THEN 'Operating Expenses'
        WHEN Account IN ('Other Revenue', 'Total Other Income', 'Profit/(Loss) on Sale of Asset') THEN 'Income'
      ELSE account_line_type
      END AS account_line_type
    FROM json_base
),

add_metadata AS (
    SELECT
      * EXCEPT (Category, account_line_type),	
      '8658a303-4b50-49ab-9b7a-887b2e2b68e7' AS tracking_category_id,
      '1f1eeb8e-5413-4594-a3d1-a9bd0691e353' AS data_file_id,
      'ACTIVE' AS option_status, 
      'Builds' AS tracking_name,
      'Rya-Dev' AS data_file_code,
      'Ryan Developments Pty Ltd' AS data_file_name,
      CASE 
        WHEN UPPER(Category) = '(A QUINNS RD WELLSFORD' THEN '9A QUINNS RD WELLSFORD VIC 3551'
        ELSE UPPER(Category)
      END AS build,
      CASE 
        WHEN account_line_type = 'Income' THEN 'Income'
        WHEN account_line_type = 'Cost of Sales' THEN 'Less Cost of Sales'
        WHEN account_line_type = 'Operating Expenses' THEN 'Less Operating Expenses'
      END as account_line_type
    FROM correct_account_line_type
),

get_tracking_option_id AS (
    SELECT
      a.*,
      trk.tracking_option_id
    FROM add_metadata a
    LEFT JOIN {{ ref('stg_accounting__tracking_categories') }} trk ON UPPER(a.build) = UPPER(trk.option_name)
),

cast_data_types AS (
    SELECT
      CAST(Account AS STRING) AS account_name,
      CAST(Amount AS FLOAT64) AS amount,
      CAST(from_date AS DATE) AS from_date,
      CAST(to_date AS DATE) AS to_date,
      CAST(account_line_type AS STRING) AS account_line_type,
      CAST(tracking_category_id AS STRING) AS tracking_category_id,
      CAST(option_status AS STRING) AS option_status, 
      CAST(tracking_name AS STRING) AS tracking_name,
      CAST(data_file_code AS STRING) AS data_file_code,
      CAST(data_file_name AS STRING) AS data_file_name,
      CAST(build AS STRING) AS build,
      CAST(tracking_option_id AS STRING) AS tracking_option_id   
    FROM get_tracking_option_id
)

SELECT * FROM cast_data_types
WHERE build != 'TOTAL'
AND account_name NOT IN ('Net Profit', 'Gross Profit')
