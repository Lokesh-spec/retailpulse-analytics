{{ config(
    materialized='incremental',
    unique_key=['product_id', 'ingestion_time'],
    alias='stg_products',
    tags=['staging', 'products']
) }}

with source as (

    select * 
    from {{ source('retailpulse_raw', 'products') }} as source

    {% if is_incremental() %}
        where source.ingestion_time >= (
            select coalesce(
                max(t.ingestion_time),
                timestamp('2020-01-01 00:00:00')
            )
            from {{ this }} as t
        )
    {% endif %}

),
renamed as (

    select
        -- identifiers
        sku as product_id,

        -- product info
        name as product_name,
        description as product_description,
        category as product_category,
        brand as product_brand,
        condition as product_condition,

        -- pricing
        msrp,
        sale as sale_price,
        discount,
        currency,

        -- inventory
        stock_quantity,
        stock_status,
        stock_code as warehouse_code,
        stock_name as warehouse_name,
        stock_region as warehouse_region,
        stock_reorder_point as reorder_point,
        stock_last_restocked,

        -- supplier
        supplier_name,
        supplier_contact,
        supplier_lead_time_days as lead_time_days,

        -- reviews
        reviews_created_at,
        reviews_updated_at,
        reviews_rating as rating,
        reviews_review_count as review_count,
        reviews_tags,

        -- shipping
        shipping_free_shipping,
        shipping_estimated_days,
        shipping_weight,

        -- timestamp
        ingestion_time

    from source

)

select * from renamed