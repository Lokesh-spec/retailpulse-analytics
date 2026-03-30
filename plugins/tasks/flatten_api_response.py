import logging
import pandas as pd
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)


def _flatten_product(product: Dict[str, Any]) -> Optional[List[Any]]:
    """
    Flatten a single product dict into a list of values.

    Returns None if any critical field is missing — caller should skip
    the record and log it rather than writing nulls to BigQuery.
    """

    # --- Extract nested dicts first ---
    price = product.get('price', {})
    stock = product.get('stock', {})
    warehouse = stock.get('warehouse', {})
    supplier = product.get('supplier', {})
    reviews = product.get('reviews', {})
    shipping = product.get('shipping', {})

    sku = product.get('sku')

    # -------------------------------------------------------
    # Critical fields: skip the entire record if missing
    # -------------------------------------------------------
    required = {
        'sku': sku,
        'stock_quantity': stock.get('quantity'),
        'stock_status': stock.get('status'),
        'msrp': price.get('msrp'),
        'sale_price': price.get('sale'),
        'supplier_name': supplier.get('name'),
    }

    missing_critical = [k for k, v in required.items() if v is None]
    if missing_critical:
        logger.warning(
            f"SKU={sku or 'UNKNOWN'} missing critical fields: {missing_critical}. "
            f"Skipping record."
        )
        return None

    # -------------------------------------------------------
    # Important fields: default if missing, log warning
    # -------------------------------------------------------
    lead_time_days = supplier.get('leadTimeDays')
    if lead_time_days is None:
        logger.warning(f"SKU={sku} missing lead_time_days, defaulting to 0")
        lead_time_days = 0

    reorder_point = stock.get('reorderPoint')
    if reorder_point is None:
        logger.warning(f"SKU={sku} missing reorder_point, defaulting to 0")
        reorder_point = 0

    warehouse_region = warehouse.get('region')
    if warehouse_region is None:
        logger.warning(
            f"SKU={sku} missing warehouse_region, defaulting to UNKNOWN")
        warehouse_region = 'UNKNOWN'

    category = product.get('category')
    if category is None:
        logger.warning(f"SKU={sku} missing category, defaulting to UNKNOWN")
        category = 'UNKNOWN'

    brand = product.get('brand')
    if brand is None:
        logger.warning(f"SKU={sku} missing brand, defaulting to UNKNOWN")
        brand = 'UNKNOWN'

    rating = reviews.get('rating')
    if rating is None:
        logger.warning(f"SKU={sku} missing rating, defaulting to 0.0")
        rating = 0.0

    review_count = reviews.get('reviewCount')
    if review_count is None:
        logger.warning(f"SKU={sku} missing review_count, defaulting to 0")
        review_count = 0

    # -------------------------------------------------------
    # Nice to have: silent defaults, no warning needed
    # -------------------------------------------------------
    discount = price.get('discount') or 0
    currency = price.get('currency') or 'USD'
    free_shipping = shipping.get('freeShipping') or False

    # Normalize tags from list to comma-separated string
    tags = reviews.get('tags')
    tags = ', '.join(tags) if isinstance(tags, list) else (tags or '')

    return [
        sku,
        product.get('name'),
        product.get('description'),
        category,
        brand,
        product.get('condition'),

        price.get('msrp'),
        price.get('sale'),
        discount,
        currency,

        stock.get('quantity'),
        stock.get('status'),
        warehouse.get('code'),
        warehouse.get('name'),
        warehouse_region,
        reorder_point,
        stock.get('lastRestocked'),

        supplier.get('name'),
        supplier.get('contact'),
        lead_time_days,

        reviews.get('createdAt'),
        reviews.get('updatedAt'),
        rating,
        review_count,
        tags,

        free_shipping,
        shipping.get('estimatedDays'),
        shipping.get('weight'),
    ]


def flatten_api_data(data_lst: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Flatten a list of API responses into a single DataFrame.

    Each API response contains a 'data' list with one or more products.
    - Critical fields missing  → record is skipped and logged
    - Important fields missing → record kept, default applied, warning logged
    - Nice-to-have missing     → silent default applied
    """
    columns = [
        "sku", "name", "description", "category", "brand", "condition",
        "msrp", "sale", "discount", "currency",
        "stock_quantity", "stock_status", "stock_code", "stock_name", "stock_region",
        "stock_reorder_point", "stock_last_restocked",
        "supplier_name", "supplier_contact", "supplier_lead_time_days",
        "reviews_created_at", "reviews_updated_at", "reviews_rating",
        "reviews_review_count", "reviews_tags",
        "shipping_free_shipping", "shipping_estimated_days", "shipping_weight",
    ]

    product_lst = []
    skipped_count = 0

    for response in data_lst:
        data_items = response.get('data') or []

        if not data_items:
            logger.warning("API response contained no data items, skipping.")
            continue

        for product in data_items:

            # Guard: must be a non-empty dict
            if not product or not isinstance(product, dict):
                logger.warning(
                    f"Invalid product record type: {type(product)}, skipping.")
                skipped_count += 1
                continue

            try:
                result = _flatten_product(product)

                # Explicitly check None — don't use `if result` because
                # a valid row with falsy values (0, False) would be skipped
                if result is not None:
                    product_lst.append(result)
                else:
                    skipped_count += 1

            except Exception as e:
                sku = product.get('sku', 'UNKNOWN')
                logger.error(f"Unexpected error flattening SKU={sku}: {e}")
                skipped_count += 1
                continue

    logger.info(
        f"Flattening complete — success: {len(product_lst)}, skipped: {skipped_count}")

    if not product_lst:
        logger.warning(
            "No products were flattened — returning empty DataFrame.")
        return pd.DataFrame(columns=columns)

    return pd.DataFrame(product_lst, columns=columns)
