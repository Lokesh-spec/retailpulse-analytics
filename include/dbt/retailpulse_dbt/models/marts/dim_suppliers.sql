{{
    config(
        materialized='table',
        alias='dim_suppliers',
        tags=['dimension', 'supplier']
    )
}}

WITH latest_supplier AS (

    SELECT
        supplier_name,
        supplier_contact,
        lead_time_days,
        ingestion_time,

        ROW_NUMBER() OVER (
            PARTITION BY supplier_name
            ORDER BY ingestion_time DESC
        ) AS rn

    FROM {{ ref('stg_products') }}

)

SELECT
    supplier_name,
    supplier_contact,
    lead_time_days

FROM latest_supplier
WHERE rn = 1