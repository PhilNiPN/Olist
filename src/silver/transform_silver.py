"""
silver layer transformation module.

Transforms the SQL files in the src/silver/sql directory.
This module loads and exposes them as TRANSFORMS with the snapshot resolution logic. 
"""

import logging
from pathlib import Path
from psycopg2 import extensions
from .config import SILVER_TABLE_SOURCES, TABLE_TO_FILE

logger = logging.getLogger(__name__)
SQL_DIR = Path(__file__).parent / 'sql'

def _load_transforms() -> dict[str, str]:
    """ 
    load and compile all SQL transforms from the sql directory. 
    Keyed by table name.
    """
    transforms = {}
    for table_name in SILVER_TABLE_SOURCES:
        sql_file = SQL_DIR / f"{table_name}.sql"
        if not sql_file.exists():
            raise FileNotFoundError(f"Missing transform: {sql_file}")
        transforms[table_name] = sql_file.read_text(encoding = 'utf-8')
    return transforms

TRANSFORMS = _load_transforms()

def resolve_effective_snapshot(conn: extensions.connection, target_snapshot_id: str) -> dict[str, str]:
    """
    For each bronze table, find the snapshot_id that was most recently loaded and includes target_snapshot_id.
    Returns {bronze_table_name: effective_snapshot_id}. 
    Raises ValueError if any required bronze_table has never been loaded.
    """
    all_bronze_tables = set()
    for sources in SILVER_TABLE_SOURCES.values():
        all_bronze_tables.update(sources)

    filenames = [TABLE_TO_FILE[t] for t in all_bronze_tables]

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (filename) filename, snapshot_id
            FROM ingestion.file_manifest
            WHERE filename = ANY(%s)
                AND created_at <= (
                        SELECT MAX(created_at)
                        FROM ingestion.file_manifest
                        WHERE snapshot_id <= %s
                    )
            ORDER BY filename, created_at DESC
            """,
            (filenames, target_snapshot_id),
        )
        rows = cur.fetchall()

    file_to_table = {val: k for k, val in TABLE_TO_FILE.items()}
    effective = {file_to_table[filename]: snap for filename, snap in rows}

    missing = all_bronze_tables - effective.keys()
    if missing:
        raise ValueError(
            f"No bronze data found for tables: {missing}" 
            f"Run the bronze pipeline to load the missing data."
        )
    
    logger.info('effective_snapshots_resolved', extra={'target': target_snapshot_id, 'effective':effective}
    )
    return effective