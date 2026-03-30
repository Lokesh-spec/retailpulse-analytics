{{
    config(
        materialized='table',
        alias='dim_dates',
        tags=['dimension', 'date']
    )
}}

WITH dates AS (

    SELECT DISTINCT
        DATE(ingestion_time) AS full_date
    FROM {{ ref('stg_products') }}

),

final AS (

    SELECT
        FORMAT_DATE('%Y%m%d', full_date) AS date_key,
        full_date,
        EXTRACT(DAY FROM full_date) AS day,
        EXTRACT(MONTH FROM full_date) AS month,
        EXTRACT(YEAR FROM full_date) AS year,
        EXTRACT(DAYOFWEEK FROM full_date) AS day_of_week

    FROM dates

)

SELECT * FROM final