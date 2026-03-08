"""
Silver layer configuration.
"""

SILVER_TABLE_SOURCES = {
    'orders': ['orders'],
    'order_items': ['order_items'],
    'customers': ['customers'],
    'products': ['products', 'product_category_name_translation'],
    'sellers': ['sellers'],
    'order_reviews': ['order_reviews'],
    'order_payments': ['order_payments'],
    'geolocation': ['geolocation'],
}

TABLE_TO_FILE = {
    'orders': 'olist_orders_dataset.csv',
    'order_items': 'olist_order_items_dataset.csv',
    'customers': 'olist_customers_dataset.csv',
    'products': 'olist_products_dataset.csv',
    'sellers': 'olist_sellers_dataset.csv',
    'order_reviews': 'olist_order_reviews_dataset.csv',
    'order_payments': 'olist_order_payments_dataset.csv',
    'geolocation': 'olist_geolocation_dataset.csv',
    'product_category_name_translation': 'product_category_name_translation.csv',
}