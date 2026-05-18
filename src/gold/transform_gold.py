"""
Gold layer transformation module.

Loads the SQL files from src/gold/sql/ and exposes them as TRANSFORMS.
Unlike silver, gold reads from silver tables (not bronze), so snapshot
resolution is simpler — we just need the silver snapshot_id, not
per-file effective snapshots.
"""

import logging
from pathlib import Path
from .config import LOAD_ORDER

logger = logging.getLogger(__name__)
SQL_DIR = Path(__file__).parent / 'sql'


def _load_transforms() -> dict[str, str]:
    """
    Load all gold SQL transforms from disk, keyed by table name.
    Validates at import time that every table in LOAD_ORDER has a .sql file.
    """
    transforms = {}
    for table_name in LOAD_ORDER:
        sql_file = SQL_DIR / f"{table_name}.sql"
        if not sql_file.exists():
            raise FileNotFoundError(f"Missing gold transform: {sql_file}")
        transforms[table_name] = sql_file.read_text(encoding='utf-8')
    return transforms


TRANSFORMS = _load_transforms()
