"""
Bronze layer quality checks.
"""

import logging
from dataclasses import dataclass
from psycopg2 import sql, extensions

logger = logging.getLogger(__name__)

@dataclass
class QualityResult:
    table: str
    check_name: str
    passed: bool
    details: dict


# columns that must not be entirely empty pr table
PRIMARY_KEYS = {
    "orders": ["order_id"],
    "order_items": ["order_id", "order_item_id"],
    "customers": ["customer_id"],
    "products": ["product_id"],
    "sellers": ["seller_id"],
    "order_reviews": ["review_id"],
    "order_payments": ["order_id"],
    "geolocation": ["geolocation_zip_code_prefix"],
    "product_category_name_translation": ["product_category_name"],
}

def check_row_count(conn: extensions.connection, table_name: str, snapshot_id: str, expected_rows: int | None) -> QualityResult:
    """ check if the row count matches the expected value from manifest. """
    if expected_rows is None:
        return QualityResult(
            table = table_name,
            check_name = 'row_count',
            passed = True,
            details = {'skipped': True, 'reason': 'no manifest row count'},
        )

    target = sql.Identifier('bronze', table_name)
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT COUNT(*) FROM {} WHERE _snapshot_id = %s").format(target), (snapshot_id,)
        )
        actual = cur.fetchone()[0]

    passed = actual == expected_rows
    result = QualityResult(
        table = table_name,
        check_name = 'row_count',
        passed = passed,
        details = {'expected': expected_rows, 'actual': actual}
    )

    if not passed:
        logger.warning('dq_row_count_mismatch', extra = {
            'table': table_name,
            **result.details,
        })
    return result


def check_not_empty(conn:extensions.connection, table_name: str, snapshot_id: str) -> QualityResult:
    """ Verify table has at least one row for this snapshot. """
    target = sql.Identifier('bronze', table_name)
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT COUNT(*) FROM {} WHERE _snapshot_id = %s").format(target), (snapshot_id,)
        )
        count = cur.fetchone()[0]
    
    passed = count > 0
    result = QualityResult(
        table = table_name,
        check_name = 'not_empty',
        passed = passed, 
        details = {'row_count': count},
    )
    if not passed: 
        logger.error('dq_table_empty', extra={'table': table_name})
    return result

def check_primary_key_nulls(conn: extensions.connection, table_name: str, snapshot_id: str) -> list[QualityResult]:
    """ check null rate on primary key columns for this snapshot. """
    results = []
    pk_cols = PRIMARY_KEYS.get(table_name, [])
    target = sql.Identifier('bronze', table_name)

    for col in pk_cols: 
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
                    SELECT COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE {} IS NULL) AS null_count
                    FROM {}
                    WHERE _snapshot_id = %s 
                    """
                ).format(sql.Identifier(col), target), (snapshot_id,)
            )
            total, null_count = cur.fetchone()
        null_rate = null_count / total if total > 0 else 0
        passed = null_count == 0

        result = QualityResult(
            table = table_name,
            check_name = f"pk_null_{col}",
            passed = passed, 
            details = {
                'column': col,
                'total': total,
                'null_count': null_count,
                'null_rate': round(null_rate, 4),
            }
        )
        if not passed: 
            logger.warning('dq_pk_nulls', extra={'table': table_name, **result.details})
        results.append(result)
    
    return results


def check_schema(conn: extensions.connection, table_name: str) -> QualityResult:
    """ verify bronze table schema has the expected columns. """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'bronze' AND table_name = %s
            ORDER BY ordinal_position
            """, (table_name,)
        )
        actual_cols = {row[0] for row in cur.fetchall()}

    # primary keys and metadata columns should always exist
    pk_cols = set(PRIMARY_KEYS.get(table_name, []))
    metadata = {'_snapshot_id', '_run_id', '_inserted_at', '_source_file'}
    expected = pk_cols | metadata
    missing = expected - actual_cols
    passed = len(missing) == 0

    result = QualityResult(
        table = table_name,
        check_name = 'schema',
        passed = passed,
        details = {
            'missing_columns': list(missing), 
            'actual_columns': list(actual_cols),
        }
    )
    if not passed:
        logger.error('dq_schema_mismatch', extra = {
            'table': table_name, 
            'missing': list(missing),
            }
        )
    return result

def run_quality_checks(conn: extensions.connection, table_name: str, snapshot_id: str, expected_rows: int | None) -> list[QualityResult]:
    """ run all bronze layer quality checks for a table and persist results. """
    results = []
    results.append(check_not_empty(conn, table_name, snapshot_id))
    results.append(check_row_count(conn, table_name, snapshot_id, expected_rows))
    results.append(check_schema(conn, table_name))
    results.extend(check_primary_key_nulls(conn, table_name, snapshot_id))
    
    return results

def persist_quality_results(conn: extensions.connection, run_id: str, results: list[QualityResult]):
    """ Write quality check results to the database. """
    import json
    with conn.cursor() as cur:
        for res in results:
            cur.execute(
                """
                INSERT INTO ingestion.quality_checks (run_id, table_name, check_name, passed, details)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (run_id, table_name, check_name) DO UPDATE
                SET passed = EXCLUDED.passed, details = EXCLUDED.details, checked_at = NOW()
                """, (run_id, res.table, res.check_name, res.passed, json.dumps(res.details))
            )
    conn.commit()
