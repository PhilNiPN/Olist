"""
Load silver tables into gold tables.

Follows the same patterns as silver.load_silver:
  - Idempotent: DELETE by _snapshot_id before INSERT.
  - SAVEPOINT per table: a DQ-rejected or failed table is rolled back
    without losing the metadata we already wrote for it.
  - Atomic commits: data + lineage + quality results + table-load status
    are committed together per table.
  - Cross-table quality checks run after all tables are loaded.
  - Notification dispatched at the end.

Key difference from silver: gold reads from silver (same DB), so there
is no file-manifest or effective-snapshot resolution per file. We just
need the snapshot_id that was loaded into silver.
"""

import uuid
import logging
from psycopg2 import sql, extensions
from dataclasses import dataclass

from db import get_db_connection, health_check
from .transform_gold import TRANSFORMS
from .config import LOAD_ORDER, GOLD_TABLE_SOURCES
from .quality_gold import run_quality_checks, run_cross_table_checks, persist_quality_results
from notification import PipelineOutcome, notify

logger = logging.getLogger(__name__)


@dataclass
class GoldLoadSummary:
    run_id: str
    snapshot_id: str
    tables_loaded: int
    tables_failed: int
    tables_rejected: int


# ── ingestion bookkeeping ────────────────────────────────────────────

def _register_run(conn: extensions.connection, run_id: str, snapshot_id: str):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ingestion.runs (run_id, snapshot_id, layer, status)
            VALUES (%s, %s, 'gold', 'started')
            """, (run_id, snapshot_id),
        )
    # Commit immediately so monitoring can see the run was attempted
    conn.commit()


def _complete_run(conn: extensions.connection, run_id: str, status: str, error_message: str = None):
    # No commit — caller decides when to commit
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ingestion.runs
            SET status = %s, end_time = NOW(), error_message = %s
            WHERE run_id = %s
            """, (status, error_message, run_id),
        )


def _register_table_load(conn: extensions.connection, run_id: str, gold_table: str):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ingestion.gold_table_loads (run_id, gold_table, status)
            VALUES (%s, %s, 'pending')
            ON CONFLICT (run_id, gold_table) DO NOTHING
            """, (run_id, gold_table),
        )


def _complete_table_load(conn: extensions.connection, run_id: str,
                         gold_table: str, status: str, rows_inserted: int = 0, message: str = None):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ingestion.gold_table_loads
            SET status = %s, rows_inserted = %s, message = %s, updated_at = NOW()
            WHERE run_id = %s AND gold_table = %s
            """, (status, rows_inserted, message, run_id, gold_table),
        )


def _get_completed_tables(conn: extensions.connection, run_id: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT gold_table
            FROM ingestion.gold_table_loads
            WHERE run_id = %s AND status = 'loaded'
            """, (run_id,),
        )
        return {row[0] for row in cur.fetchall()}


def _record_lineage(conn: extensions.connection, run_id: str,
                    gold_table: str, snapshot_id: str):
    """Record which silver tables (at which snapshot) fed this gold table."""
    silver_sources = GOLD_TABLE_SOURCES[gold_table]
    with conn.cursor() as cur:
        for silver_table in silver_sources:
            cur.execute(
                """
                INSERT INTO ingestion.gold_lineage (run_id, gold_table, silver_table, effective_snapshot_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (run_id, gold_table, silver_table) DO UPDATE
                SET effective_snapshot_id = EXCLUDED.effective_snapshot_id
                """,
                (run_id, gold_table, silver_table, snapshot_id),
            )


def _resolve_snapshot(conn: extensions.connection, snapshot_id: str = None) -> str:
    """
    If no snapshot_id is given, find the latest one that was successfully
    loaded into silver. This is the gold layer's equivalent of silver's
    resolve_effective_snapshot, but simpler because we just need one ID.
    """
    if snapshot_id:
        return snapshot_id

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT snapshot_id
            FROM ingestion.runs
            WHERE layer = 'silver' AND status IN ('success', 'success_with_warnings')
            ORDER BY start_time DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()

    if not row:
        raise ValueError(
            "No successful silver run found. Run the silver pipeline first."
        )
    return row[0]


# ── main load function ───────────────────────────────────────────────

def load(snapshot_id: str = None, run_id: str = None, resume: bool = False) -> GoldLoadSummary:
    """
    Load all gold tables from silver for the given snapshot_id.
    Tables are loaded in LOAD_ORDER (dims before fact).
    """
    run_id = run_id or str(uuid.uuid4())

    status = health_check()
    if status['status'] != 'healthy':
        raise RuntimeError(f"database is unhealthy: {status}")

    with get_db_connection() as conn:
        snapshot_id = _resolve_snapshot(conn, snapshot_id)
        logger.info('gold_snapshot_resolved', extra={'snapshot_id': snapshot_id})

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

        # LOAD_ORDER guarantees dims are populated before the fact table
        for gold_table in LOAD_ORDER:
            transform_sql = TRANSFORMS[gold_table]

            if gold_table in completed:
                logger.info('table_skipped_already_loaded', extra={'table': gold_table})
                loaded.append(gold_table)
                continue

            _register_table_load(conn, run_id, gold_table)
            try:
                params = {'snapshot_id': snapshot_id, 'run_id': run_id}

                with conn.cursor() as cur:
                    cur.execute("SAVEPOINT before_gold_data")

                    # Idempotency: clear previous data for this snapshot.
                    # dim_dates uses ON CONFLICT in its SQL and has no _snapshot_id,
                    # so we skip the delete for it.
                    if gold_table != 'dim_dates':
                        cur.execute(
                            sql.SQL("DELETE FROM {} WHERE _snapshot_id = %s").format(
                                sql.Identifier('gold', gold_table)
                            ), (snapshot_id,),
                        )

                    cur.execute(transform_sql, params)
                    rows = cur.rowcount

                logger.info('gold_table_loaded', extra={'table': gold_table, 'rows': rows})

                # Run per-table quality checks
                dq_results = run_quality_checks(conn, gold_table, snapshot_id)
                critical_failures = [r for r in dq_results if not r.passed and r.severity == 'error']

                if critical_failures:
                    with conn.cursor() as cur:
                        cur.execute("ROLLBACK TO SAVEPOINT before_gold_data")

                    persist_quality_results(conn, run_id, dq_results)
                    _complete_table_load(conn, run_id, gold_table, 'dq_rejected',
                        message=f"rolled back: {[r.check_name for r in critical_failures]}")
                    conn.commit()

                    dq_rejected.append(gold_table)
                    all_dq_failures.extend(critical_failures)
                    logger.error('gold_table_dq_rejected', extra={
                        'table': gold_table,
                        'checks': [r.check_name for r in critical_failures],
                    })

                else:
                    with conn.cursor() as cur:
                        cur.execute("RELEASE SAVEPOINT before_gold_data")

                    _record_lineage(conn, run_id, gold_table, snapshot_id)
                    persist_quality_results(conn, run_id, dq_results)
                    _complete_table_load(conn, run_id, gold_table, 'loaded', rows)
                    conn.commit()

                    loaded.append(gold_table)
                    warning_failures = [r for r in dq_results if not r.passed]
                    if warning_failures:
                        all_dq_failures.extend(warning_failures)
                        logger.warning('gold_dq_warnings', extra={
                            'table': gold_table,
                            'checks': [r.check_name for r in warning_failures],
                        })

            except Exception as e:
                with conn.cursor() as cur:
                    cur.execute("ROLLBACK TO SAVEPOINT before_gold_data")

                _complete_table_load(conn, run_id, gold_table, 'failed', message=str(e))
                conn.commit()

                failed.append(gold_table)
                logger.error('gold_table_failed', extra={'table': gold_table, 'error': str(e)}, exc_info=True)

        # Cross-table checks only make sense if at least the fact table loaded
        if 'fact_order_items' in loaded:
            cross_results = run_cross_table_checks(conn, snapshot_id)
            persist_quality_results(conn, run_id, cross_results)
            conn.commit()

            cross_failed = [r for r in cross_results if not r.passed and r.severity == 'error']
            all_dq_failures.extend(cross_failed)

            if cross_failed:
                logger.warning('gold_cross_table_dq_failed', extra={
                    'checks': [(r.table, r.check_name) for r in cross_failed],
                })

        # Determine final run status
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

        _complete_run(conn, run_id, run_status, error_msg)
        conn.commit()

        notify(PipelineOutcome(
            run_id=run_id,
            layer='gold',
            status=run_status,
            tables_loaded=len(loaded),
            tables_failed=len(failed),
            tables_rejected=len(dq_rejected),
            dq_failures=[
                {'table': r.table, 'check': r.check_name, 'details': r.details}
                for r in all_dq_failures
            ],
        ))

    return GoldLoadSummary(
        run_id=run_id,
        snapshot_id=snapshot_id,
        tables_loaded=len(loaded),
        tables_failed=len(failed),
        tables_rejected=len(dq_rejected),
    )
