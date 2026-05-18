"""
Gold layer configuration.

GOLD_TABLE_SOURCES maps each gold table to the silver tables it reads from.
Order matters: dimensions are loaded before the fact table because the fact
table's quality checks verify FK references into the dimensions.

LOAD_ORDER is an explicit list so the pipeline always processes dims first.
"""

# Which silver tables feed each gold table.
# Used by the loader to record lineage and resolve effective snapshots.
GOLD_TABLE_SOURCES = {
    'dim_dates':      ['orders'],
    'dim_customers':  ['customers', 'orders'],
    'dim_products':   ['products'],
    'dim_sellers':    ['sellers'],
    'fact_order_items': [
        'orders', 'order_items', 'customers',
        'order_reviews', 'order_payments',
    ],
}

# Dimensions first, then facts — the fact table depends on all four dims.
LOAD_ORDER = [
    'dim_dates',
    'dim_customers',
    'dim_products',
    'dim_sellers',
    'fact_order_items',
]
