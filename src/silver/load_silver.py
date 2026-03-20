"""
Load bronze tables into silver tables.
"""

import uuid
import logging
from psycopg2 import sql, extensions
from dataclasses import dataclass

from db import get_db_connection, health_check
from .transform_silver import TRANSFORMS, resolve_effective_snapshot
from .config import SILVER_TABLE_SOURCES
from .quality_silver import run_quality_checks, run_cross_table_checks, persist_quality_results
from notification import PipelineOutcome, notify

logger = logging.getLogger(__name__)

@dataclass
class SilverLoadSummary:
    run_id: str
    snapshot_id: str
    tables_loaded: int
    tables_failed: int
    tables_rejected: int
    effective_snapshot_id: dict[str, str]


def _register_run(conn:extensions.connection, run_id:str, snapshot_id:str):
    # keep conn.commit() here to ensure monitoring can see a run was attempted.
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ingestion.runs (run_id, snapshot_id, layer, status) 
            VALUES (%s, %s, 'silver', 'started')
            """, (run_id, snapshot_id),
        )
    conn.commit()

def _complete_run(conn:extensions.connection, run_id:str, status:str, error_message:str=None):
    # No conn.commit() on purpose so that run completion is committed atomically as one unit.
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ingestion.runs
            SET status = %s, end_time = NOW(), error_message = %s
            WHERE run_id = %s
            """, (status, error_message, run_id),
        )

def _register_table_load(conn: extensions.connection, run_id: str, silver_table: str):
    # No conn.commit() on purpose so that table registration, data load, 
    # lineage, and quality results are committed atomically as one unit.
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ingestion.silver_table_loads (run_id, silver_table, status)
            VALUES (%s, %s, 'pending')
            ON CONFLICT (run_id, silver_table) DO NOTHING
            """, (run_id, silver_table),
        )

def _complete_table_load(conn: extensions.connection, run_id: str,
    silver_table: str, status: str, rows_inserted: int = 0, message: str = None):
    # No conn.commit() on purpose so that table registration, data load, 
    # lineage, and quality results are committed atomically as one unit.
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ingestion.silver_table_loads
            SET status = %s, rows_inserted = %s, message = %s, updated_at = NOW()
            WHERE run_id = %s AND silver_table = %s
            """, (status, rows_inserted, message, run_id, silver_table),
        )

def _get_completed_tables(conn: extensions.connection, run_id: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT silver_table 
            FROM ingestion.silver_table_loads
            WHERE run_id = %s AND status = 'loaded'
            """, (run_id,),
        )
        return {row[0] for row in cur.fetchall()}

def _record_lineage(conn: extensions.connection, run_id:str,
    silver_table:str, effective_snapshots: dict[str, str]):
    # No conn.commit() on purpose so that table registration, data load, 
    # lineage, and quality results are committed atomically as one unit.
    bronze_sources = SILVER_TABLE_SOURCES[silver_table]
    with conn.cursor() as cur:
        for bronze_table in bronze_sources: 
            cur.execute(
                """
                INSERT INTO ingestion.silver_lineage (run_id, silver_table, bronze_table, effective_snapshot_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (run_id, silver_table, bronze_table) DO UPDATE
                SET effective_snapshot_id = EXCLUDED.effective_snapshot_id
                """, 
                (run_id, silver_table, bronze_table, effective_snapshots[bronze_table]),
            )


def _build_query_params(target_snapshot_id:str, run_id:str, 
    silver_table:str, effective_snapshots: dict[str, str]) -> dict:
    """
    Builds a dict of query parameters shared by all the SQL template.
    Includes the target snapshot_id, run_id, and the effective snapshot_id for each bronze table.
    """
    params = {'target_snapshot_id': target_snapshot_id, 'run_id': run_id}
    for bronze_table in SILVER_TABLE_SOURCES[silver_table]:
        params[f"eff_{bronze_table}"] = effective_snapshots[bronze_table]
    return params


### Main load function 

def load(snapshot_id: str = None, run_id: str = None, resume: bool = False) -> SilverLoadSummary:
    """
    Load all silver tables from bronze for the given snapshot_id.
    """

    if snapshot_id is None:
        from bronze.config import latest_manifest_path
        import json
        path = latest_manifest_path()
        if path is None:
            raise FileNotFoundError('No manifest found, try running bronze pipeline first.')
        manifest = json.loads(path.read_text())
        snapshot_id = manifest['snapshot_id']

    run_id = run_id or str(uuid.uuid4())

    status = health_check()
    if status['status'] != 'healthy':
        raise RuntimeError(f"database is unhealthy: {status}")

    with get_db_connection() as conn:
        # resolve which bronze snapshot each source table should read from
        effective = resolve_effective_snapshot(conn, snapshot_id)

        completed = set()
        if resume and run_id:
            completed = _get_completed_tables(conn, run_id)
            logger.info('resuming_run', extra={
                'run_id': run_id, 'skipping': list(completed),
            })
        else:
            _register_run(conn, run_id, snapshot_id)

        loaded = []
        failed = []
        dq_rejected = []
        all_dq_failures = []

        for silver_table, transform_sql in TRANSFORMS.items():
            if silver_table in completed:
                logger.info('table_skipped_already_loaded', extra={'table': silver_table})
                loaded.append(silver_table)
                continue


            _register_table_load(conn, run_id, silver_table)
            try:
                params = _build_query_params(snapshot_id, run_id, silver_table, effective)

                # SAVEPOINT lets us rollback only the silver data on DQfailure or exception, 
                # without discarding the metadata that we still want to commit in the same atomic unit.
                with conn.cursor() as cur:
                    cur.execute("SAVEPOINT before_silver_data")

                    # idempotency: clear previous data for this snapshot
                    cur.execute(
                        sql.SQL("DELETE FROM {} WHERE _snapshot_id = %s").format(
                            sql.Identifier('silver', silver_table)
                        ), (snapshot_id,),
                    )
                    cur.execute(transform_sql, params)
                    rows = cur.rowcount
                logger.info('silver_table_loaded', extra={'table': silver_table, 'rows': rows})

                eff_snap = effective[SILVER_TABLE_SOURCES[silver_table][0]]
                dq_results = run_quality_checks(conn, silver_table, snapshot_id, eff_snap)
                critical_failures = [r for r in dq_results if not r.passed and r.severity == 'error']

                if critical_failures:
                    # Undo only the silver data writes; everything before the savepoint stays in
                    # the transaction so we can still record rejection metadata.
                    with conn.cursor() as cur:
                        cur.execute("ROLLBACK TO SAVEPOINT before_silver_data")

                    persist_quality_results(conn, run_id, dq_results)
                    _complete_table_load(conn, run_id, silver_table, 'dq_rejected',
                        message=f"rolled back: {[r.check_name for r in critical_failures]}")

                    # Single atomic commit: table-load status + quality results
                    conn.commit()

                    dq_rejected.append(silver_table)
                    all_dq_failures.extend(critical_failures)
                    logger.error('silver_table_dq_rejected', extra={
                        'table': silver_table,
                        'checks': [r.check_name for r in critical_failures],
                    })

                else:
                    # No critical failures — release savepoint so the silver
                    # data becomes part of the main transaction.
                    with conn.cursor() as cur:
                        cur.execute("RELEASE SAVEPOINT before_silver_data")

                    _record_lineage(conn, run_id, silver_table, effective)
                    persist_quality_results(conn, run_id, dq_results)
                    _complete_table_load(conn, run_id, silver_table, 'loaded', rows)

                    # Single atomic commit: silver data + lineage +
                    # quality results + table-load status all at once.
                    conn.commit()

                    loaded.append(silver_table)
                    warning_failures = [r for r in dq_results if not r.passed]

                    if warning_failures:
                        all_dq_failures.extend(warning_failures)
                        logger.warning('silver_dq_warnings', extra={
                            'table': silver_table,
                            'checks': [r.check_name for r in warning_failures],
                        })

            except Exception as e:
                # Undo silver data but keep metadata in the transaction
                # so we can continue the failure status atomically.
                with conn.cursor() as cur:
                    cur.execute("ROLLBACK TO SAVEPOINT before_silver_data")

                _complete_table_load(conn, run_id, silver_table, 'failed', message=str(e))
                conn.commit()

                failed.append(silver_table)
                logger.error('silver_table_failed', extra={'table': silver_table, 'error': str(e)}, exc_info=True)

        if loaded:
            cross_results = run_cross_table_checks(conn, snapshot_id)
            persist_quality_results(conn, run_id, cross_results)
            
            # we commit the cross-table results here to ensure the atomicity of the transaction.
            conn.commit()

            cross_failed = [r for r in cross_results if not r.passed and r.severity == 'error']
            all_dq_failures.extend(cross_failed)

            if cross_failed:
                logger.warning('silver_cross_table_dq_failed', extra = {
                    'checks': [(r.table, r.check_name) for r in cross_failed],
                })
        
        if failed:
            run_status = 'failed'
        elif all_dq_failures:
            run_status = 'success_with_warnings'
        else:
            run_status = 'success'

        error_msg = None
        if failed:
            error_msg = f"failed tables: {failed}"
        elif dq_rejected:
            error_msg = f"dq rejected tables: {dq_rejected}"

        # mark run complete
        _complete_run(conn, run_id, run_status, error_msg)
        conn.commit()

        notify(PipelineOutcome(
            run_id=run_id,
            layer='silver',
            status=run_status,
            tables_loaded=len(loaded),
            tables_failed=len(failed),
            tables_rejected=len(dq_rejected),
            dq_failures=[
                {'table': r.table, 'check': r.check_name, 'details': r.details}
                for r in all_dq_failures
            ],
        ))

    return SilverLoadSummary(
        run_id = run_id,
        snapshot_id = snapshot_id,
        tables_loaded = len(loaded),
        tables_failed = len(failed),
        tables_rejected = len(dq_rejected),
        effective_snapshot_id = effective,
    )