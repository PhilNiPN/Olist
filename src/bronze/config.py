"""Bronze layer configuration."""

from pathlib import Path

# Paths
DATA_DIR = Path("Data")
RAW_BASE = DATA_DIR / "raw"
MANIFEST_DIR = DATA_DIR / "manifest"

def raw_dir(snapshot_id: str) -> Path:
    """ returns snapshot-specific raw data directory. """
    return RAW_BASE / snapshot_id

# Kaggle dataset
KAGGLE_DATASET = "olistbr/brazilian-ecommerce"

# CSV filename → bronze table mapping
FILE_TO_TABLE = {
    "olist_orders_dataset.csv": "orders",
    "olist_order_items_dataset.csv": "order_items",
    "olist_customers_dataset.csv": "customers",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "olist_order_reviews_dataset.csv": "order_reviews",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_geolocation_dataset.csv": "geolocation",
    "product_category_name_translation.csv": "product_category_name_translation",
}

def manifest_path(snapshot_id: str) -> Path:
    """ returns the path for a specific snapshots manifest. """
    return MANIFEST_DIR / f"{snapshot_id}.json"

def latest_manifest_path() -> Path | None:
    """ returns the most recent modified manifest file or none. """
    MANIFEST_DIR.mkdir(parents=True, exist_ok=True)
    manifests = sorted(MANIFEST_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime)
    return manifests[-1] if manifests else None
