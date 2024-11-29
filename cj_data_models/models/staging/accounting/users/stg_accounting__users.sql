{{ config(
    materialized='view',
    tags=["module:accounting", "submodule:users"]
) }}

WITH base_data AS (
    SELECT *
    FROM {{ ref('base_accounting__users') }}  -- Replace with your base model reference
),

unnested_data AS (
    SELECT 
        -- Extract top-level fields from `value`
        main.OrganisationRole AS organisation_role,
        main.IsSubscriber AS is_subscriber,
        main.LastName AS last_name,
        main.EmailAddress AS email_address,
        main.UpdatedDateUTC AS updated_date_utc,
        main.UserID AS user_id,
        main.GlobalUserID AS global_user_id,
        main.FirstName AS first_name,

        -- Extract fields from nested `DataFile`
        main.DataFile.DataFileCode AS data_file_code,
        main.DataFile.DataFileName AS data_file_name,
        main.DataFile.DataFileID AS data_file_id
    FROM base_data,
    UNNEST(value) AS main  -- Unnest `value` field
)

SELECT *
FROM unnested_data
