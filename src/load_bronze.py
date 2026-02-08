"""
Laod raw csv into bronze layer
"""

import json
import uuid
import logging
from dataclasses import dataclass
from typing import List

from config import RAW_DIR, FILE_TO_TABLE, manifest_path, latest_manifest_path
from db import get_db_connection, load_csv_via_temp_table, LoadResult, health_check

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class LoadSummary:
    """ summary of a bronze layer load run. """
    run_id: str
    snapshot_id: str
    tables_loaded: int
    total_rows: int
    results: List[LoadResult]

def _register_run(conn, run_id: str, snapshot_id: str):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO ingestion.runs (run_id, snapshot_id, status) VALUES (%s, %s, 'started')",
            (run_id, snapshot_id)
        )
    conn.commit()

def _complete_run(conn, run_id: str, status: str):
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE ingestion.runs SET status = %s, end_time = NOW() WHERE run_id = %s",
            (status, run_id)
        )
    conn.commit()

def load(snapshot_id: str = None, run_id: str = None) -> LoadSummary:
    """ load all csv from manifest into the bronze tables. """

    # if snapshot_id is not provided, then read it from manifest
    if snapshot_id is None: 
        path = latest_manifest_path()
        if path is None:
            raise FileNotFoundError("No manifest found, try extract first.")
        manifest = json.loads(path.read_text())
        snapshot_id = manifest['snapshot_id']
    
    run_id = run_id or str(uuid.uuid4())
    results = []

    # database health check
    status = health_check()
    if status['status'] != 'healthy':
        raise RuntimeError(f"database is unhealthy: {status}")

    with get_db_connection() as conn:
        for filename, table_name in FILE_TO_TABLE.items():
            filepath = RAW_DIR / filename
            if not filepath.exists():
                logger.warning(f"Skipping missing file: {filepath}")
                continue

            result = load_csv_via_temp_table(conn, str(filepath), table_name, snapshot_id, run_id)
            results.append(result)
            logger.info(f"Loaded {table_name}: {result.rows_inserted} rows")

    return LoadSummary(
        run_id=run_id,
        snapshot_id=snapshot_id,
        tables_loaded=len(results),
        total_rows=sum(r.rows_inserted for r in results),
        results=results,
    )

if __name__ == "__main__":
    summary = load()
    print(f"Loaded {summary.total_rows} rows across {summary.tables_loaded} tables")
