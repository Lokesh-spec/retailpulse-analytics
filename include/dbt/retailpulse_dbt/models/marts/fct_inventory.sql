{{ 
    config(
        materialized='incremental',
        unique_key=['product_id', 'ingestion_time'],
        alias='fct_inventory',
        tags=['fact', 'inventory']
    ) 
}}

WITH base AS (

    SELECT
        product_id,
        supplier_name,
        stock_quantity,
        reorder_point,
        normalized_stock_status,
        msrp,
        sale_price,
        discount,
        alert_flag,
        in_stock_risk_flag,
        backorder_delay_flag,
        high_demand_flag,
        inventory_risk_flag,
        days_since_last_restock,
        lead_time_days,
        warehouse_region,
        ingestion_time
    FROM {{ ref('int_inventory_signals') }}

    {% if is_incremental() %}
    WHERE ingestion_time > (
        SELECT COALESCE(
            MAX(t.ingestion_time),
            TIMESTAMP('2020-01-01 00:00:00')
        )
        FROM {{ this }} AS t
    )
    {% endif %}

),

deduped AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY product_id, ingestion_time
            ORDER BY ingestion_time DESC
        ) AS rn
    FROM base

)

SELECT
    product_id,
    supplier_name,

    CAST(FORMAT_DATE('%Y%m%d', DATE(ingestion_time)) AS INT64) AS date_key,

    stock_quantity,
    reorder_point,
    normalized_stock_status,

    msrp,
    sale_price,
    discount,

    alert_flag,
    in_stock_risk_flag,
    backorder_delay_flag,
    high_demand_flag,
    inventory_risk_flag,

    ingestion_time,
    days_since_last_restock,

    lead_time_days,
    warehouse_region

FROM deduped
WHERE rn = 1