{{ 
    config(
        materialized='incremental',
        unique_key=['product_id', 'warehouse_region', 'ingestion_time'],
        alias='int_inventory_signals',
        tags=['inventory', 'signals']
    ) 
}}

WITH source_products AS (

    SELECT 
        product_id,
        product_name,
        product_category,
        product_brand,

        stock_quantity,
        stock_status,
        reorder_point,
        warehouse_code,
        warehouse_region,

        supplier_name,
        lead_time_days,

        msrp,
        sale_price,
        discount,

        rating,
        review_count,

        ingestion_time,
        stock_last_restocked

    FROM {{ ref('stg_products') }}

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

base_products AS (

    SELECT 
        *,
        TIMESTAMP_DIFF(ingestion_time, stock_last_restocked, DAY) AS days_since_last_restock
    FROM source_products

),

inventory_signals AS (

    SELECT
        *,

        CASE
            WHEN stock_status = 'Out of Stock' THEN 'OUT_OF_STOCK'
            WHEN stock_status = 'Low Stock' THEN 'LOW_STOCK'
            WHEN stock_status = 'In Stock' THEN 'IN_STOCK'
            WHEN stock_status = 'Backordered' THEN 'BACKORDER'
            ELSE 'DISCONTINUED'
        END AS normalized_stock_status,

        CASE
            WHEN stock_status IN ('Low Stock', 'Out of Stock') THEN TRUE
            ELSE FALSE
        END AS alert_flag,

        CASE
            WHEN stock_status = 'In Stock' 
                 AND stock_quantity < 100 
                 AND lead_time_days > 10
            THEN TRUE
            ELSE FALSE
        END AS in_stock_risk_flag,

        CASE
            WHEN stock_status = 'Backordered' AND lead_time_days > 10 THEN TRUE
            ELSE FALSE
        END AS backorder_delay_flag,

        CASE 
            WHEN days_since_last_restock < 7 THEN 'FRESH'
            WHEN days_since_last_restock BETWEEN 7 AND 30 THEN 'NORMAL'
            WHEN days_since_last_restock BETWEEN 31 AND 90 THEN 'AGING'
            ELSE 'STALE'
        END AS restock_staleness_flag,

        CASE 
            WHEN rating >= 4 AND review_count >= 500 THEN TRUE
            ELSE FALSE
        END AS high_demand_flag

    FROM base_products

),

final_inventory_signals AS (

    SELECT
        *,

        CASE 
            WHEN normalized_stock_status = 'OUT_OF_STOCK' THEN 'HIGH_RISK'
            WHEN normalized_stock_status = 'LOW_STOCK' 
                 AND high_demand_flag 
                 AND lead_time_days > 10 THEN 'HIGH_RISK'
            WHEN in_stock_risk_flag THEN 'MEDIUM_RISK'
            WHEN backorder_delay_flag THEN 'MEDIUM_RISK'
            ELSE 'LOW_RISK'
        END AS inventory_risk_flag

    FROM inventory_signals

),


deduped AS (

    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY product_id, warehouse_region, ingestion_time
            ORDER BY ingestion_time DESC
        ) AS rn
    FROM final_inventory_signals

)

SELECT *
FROM deduped
WHERE rn = 1