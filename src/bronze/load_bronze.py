"""
Load raw csv into bronze layer
"""

import json
import uuid
import logging
from dataclasses import dataclass
from typing import List

from .config import FILE_TO_TABLE, manifest_path, latest_manifest_path, raw_dir
from db import get_db_connection, load_csv_via_temp_table, LoadResult, health_check
from .quality_bronze import run_quality_checks, persist_quality_results


logger = logging.getLogger(__name__)

@dataclass
class LoadSummary:
    """ summary of a bronze layer load run. """
    run_id: str
    snapshot_id: str
    tables_loaded: int
    total_rows: int
    results: List[LoadResult]

def _file_changed(conn, filename: str, new_hash:str) -> bool:
    """ compare file hash against the last loaded hash in the database. """
    with conn.cursor() as cur: 
        cur.execute(
            """
            SELECT file_hash
            FROM ingestion.file_manifest
            WHERE filename = %s
            ORDER BY created_at DESC
            LIMIT 1
            """, (filename,))
        row = cur.fetchone()

    if row is None:
        return True # no previous hash, so we assume the file has never been loaded before 
    return row[0] != new_hash

def _record_file_manifest(conn, snapshot_id: str, filename: str, file_hash: str, file_size: int, row_count:int):
    """ record the file's metadata in the database after a successful load. """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ingestion.file_manifest (snapshot_id, filename, file_hash, file_size_bytes, row_count)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (snapshot_id, filename) DO UPDATE
            SET file_hash = EXCLUDED.file_hash,
                file_size_bytes = EXCLUDED.file_size_bytes,
                row_count = EXCLUDED.row_count
            """, (snapshot_id, filename, file_hash, file_size, row_count))
    conn.commit()

def _register_run(conn, run_id: str, snapshot_id: str):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO ingestion.runs (run_id, snapshot_id, status) VALUES (%s, %s, 'started')",
            (run_id, snapshot_id)
        )
    conn.commit()

def _complete_run(conn, run_id: str, status: str, error_message: str = None):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE ingestion.runs 
            SET status = %s, end_time = NOW(), error_message = %s 
            WHERE run_id = %s
            """,
            (status, error_message, run_id)
        )
    conn.commit()

def _register_file_load(conn, run_id: str, file_name: str):
    """ record a pending file load. """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ingestion.file_loads (run_id, filename, status)
            VALUES (%s, %s, 'pending')
            ON CONFLICT (run_id, filename) DO NOTHING
            """, (run_id, file_name)
        )
    conn.commit()

def _complete_file_load(conn, run_id: str, file_name: str, status: str, rows_inserted: int = 0, message: str = None):
    """ update file load status after attempt. """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ingestion.file_loads
            SET status = %s, rows_inserted = %s, message = %s, updated_at = NOW()
            WHERE run_id = %s AND filename = %s
            """, (status, rows_inserted, message, run_id, file_name)
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
    else: 
        from .config import manifest_path as mp
        mpath = mp(snapshot_id)
        if mpath.exists():
            manifest = json.loads(mpath.read_text())
        else:
            raise FileNotFoundError(f"No manifest found for snapshot: {snapshot_id}")
    
    snapshot_raw_dir = raw_dir(snapshot_id)

    # build a hash lookup for the files in the manifest
    file_hashes = {f['filename']: f for f in manifest['files']}
    
    run_id = run_id or str(uuid.uuid4())
    results = []
    failed_tables = []

    # database health check
    status = health_check()
    if status['status'] != 'healthy':
        raise RuntimeError(f"database is unhealthy: {status}")

    with get_db_connection() as conn:
        _register_run(conn, run_id, snapshot_id)

        for file_name, table_name in FILE_TO_TABLE.items():
            file_path = snapshot_raw_dir / file_name
            if not file_path.exists():
                logger.warning('file_missing', extra= {'filepath': str(file_path)})
                continue

            # hash check
            file_meta = file_hashes.get(file_name)
            if file_meta and not _file_changed(conn, file_name, file_meta['hash']):
                logger.info('file_skipped', extra = {'file_name': file_name, 'reason': 'hash_unchanged'})
                continue
            
            _register_file_load(conn, run_id, file_name)
            # this try/except will catch the exception, log it, and not stop the if loop.
            try:
                result = load_csv_via_temp_table(conn, str(file_path), table_name, snapshot_id, run_id, file_name)
                results.append(result)
                logger.info('table_loaded', extra= {'table': table_name, 'rows_inserted':result.rows_inserted})

                # record file manifest in database
                if file_meta:
                    _record_file_manifest(conn, snapshot_id, file_name, file_meta['hash'], file_meta['size'], result.rows_inserted)
                
                # run quality checks
                manifest_row_count = file_meta.get('row_count') if file_meta else None
                dq_results = run_quality_checks(conn, table_name, snapshot_id, manifest_row_count)
                persist_quality_results(conn, run_id, dq_results)
                
                failed_checks = [r for r in dq_results if not r.passed]
                if failed_checks:
                    logger.warning('dq_checks_failed', extra = {
                        'table': table_name,
                        'failed': [r.check_name for r in failed_checks],
                    })
                    
                _complete_file_load(conn, run_id, file_name, 'loaded', result.rows_inserted)

            except Exception as e:
                failed_tables.append(table_name)
                logger.error('table_load_failed', extra = {
                    'table': table_name,
                    'error': str(e),
                }, exc_info=True)
                _complete_file_load(conn, run_id, file_name, 'failed', message = str(e))
                # continue loading remaining tables
                
        run_status = 'failed' if failed_tables else 'success'
        error_msg = f"failed tables: {failed_tables}" if failed_tables else None
        _complete_run(conn, run_id, run_status, error_msg)

        if failed_tables:
            logger.error('run_completed_with_failures', extra={
                'run_id': run_id,
                'failed_tables': failed_tables,
                'succeeded': len(results),
                'total_tables': len(FILE_TO_TABLE),
            })    

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
