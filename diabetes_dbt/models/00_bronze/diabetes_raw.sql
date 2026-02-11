{{ config(
    materialized='view'
) }}

SELECT *
FROM {{ source('bronze', 'diabetic_data') }}