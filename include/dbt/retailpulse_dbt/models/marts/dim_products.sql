{{
    config(
        materialized='table',
        alias='dim_products',
        tags=['dimension', 'product']
    )
}}

WITH latest_products AS (

    SELECT
        product_id,
        product_name,
        product_category,
        product_brand,
        ingestion_time,

        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY ingestion_time DESC
        ) AS rn

    FROM {{ ref('stg_products') }}

)

SELECT
    product_id,
    product_name,
    product_category,
    product_brand

FROM latest_products
WHERE rn = 1