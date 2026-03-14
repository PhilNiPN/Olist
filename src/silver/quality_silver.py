"""
Quality checks for silver layer.
"""

import logging
import json
from dataclasses import dataclass
from psycopg2 import sql, extensions

logger = logging.getLogger(__name__)

@dataclass
class QualityResult:
    table: str
    check_name: str
    passed: bool
    severity: str
    details: dict

PRIMARY_KEYS = {
    'orders': ['order_id'],
    'order_items': ['order_id', 'order_item_id'],
    'customers': ['customer_id'],
    'products': ['product_id'],
    'sellers': ['seller_id'],
    'order_reviews': ['review_id', 'order_id'],
    'order_payments': ['order_id', 'payment_sequential'],
    'geolocation': ['geolocation_zip_code_prefix'],
}

CAST_COLUMNS = {
    'orders': {
        'order_purchase_ts': 'order_purchase_timestamp',
        'order_approved_at': 'order_approved_at',
        'delivered_carrier_at': 'order_delivered_carrier_date',
        'delivered_customer_at': 'order_delivered_customer_date',
        'estimated_delivery_at': 'order_estimated_delivery_date',
    },

    'order_items': {
        'order_item_id': 'order_item_id',
        'price': 'price',
        'freight_value': 'freight_value',
        'shipping_limit_date': 'shipping_limit_date',

    },

    'products': {
        'product_name_length': 'product_name_lenght',
        'product_description_length': 'product_description_lenght',
        'product_photos_qty': 'product_photos_qty',
        'product_weight_g': 'product_weight_g',
        'product_length_cm': 'product_length_cm',
        'product_height_cm': 'product_height_cm',
        'product_width_cm': 'product_width_cm',
    },

    'order_reviews': {
        'review_score': 'review_score',
        'review_creation_date': 'review_creation_date',
        'review_answer_ts': 'review_answer_timestamp',
    },

    'order_payments': {
        'payment_sequential': 'payment_sequential',
        'payment_installments': 'payment_installments',
        'payment_value': 'payment_value',
    },

    'geolocation': {
        'geolocation_lat': 'geolocation_lat',
        'geolocation_lng': 'geolocation_lng',
    },
}

REFERENTIAL_CHECKS = [
    ('order_items', 'order_id', 'orders', 'order_id'),
    ('order_items', 'product_id', 'products', 'product_id'),
    ('order_items', 'seller_id', 'sellers', 'seller_id'),
    ('order_reviews', 'order_id', 'orders', 'order_id'),
    ('order_payments', 'order_id', 'orders', 'order_id'),
    ('orders', 'customer_id', 'customers', 'customer_id'),
]


### per table quality checks

def check_not_empty(conn: extensions.connection, table_name: str, snapshot_id: str) -> QualityResult:
    """
    Check if the table is not empty.
    """
    target = sql.Identifier('silver', table_name)
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                SELECT COUNT(*)
                FROM {}
                WHERE _snapshot_id = %s
                """
                ).format(target), (snapshot_id,),
        )
        count = cur.fetchone()[0]

    passed = count > 0
    result = QualityResult(
        table = table_name, 
        check_name = 'not_empty',
        passed = passed,
        severity = 'error',
        details = {'row_count': count},
    )
    if not passed:
        logger.error('dq_silver_table_empty', extra={'table': table_name})
    return result

def check_pk_nulls(conn: extensions.connection, table_name: str, snapshot_id: str) -> list[QualityResult]:
    """
    Check if the primary key columns have any null values.
    """
    results = []
    pk_cols = PRIMARY_KEYS.get(table_name, [])
    target = sql.Identifier('silver', table_name)

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
                ).format(sql.Identifier(col), target), (snapshot_id,),
            )
            total, null_count = cur.fetchone()

        passed = null_count == 0
        results.append(QualityResult(
            table = table_name,
            check_name = f"pk_null_{col}",
            passed = passed,
            severity = 'error',
            details = {'column': col, 'total': total, 'null_count': null_count},
        ))
        if not passed: 
            logger.warning('dq_silver_pk_nulls', extra={
                'table': table_name, 
                'column': col, 
                'null_count': null_count, 
            })
    return results

def check_pk_unique(conn: extensions.connection, table_name: str, snapshot_id: str) -> list[QualityResult]:
    """
    Check if the primary key columns are unique.
    """
    results = []
    pk_cols = PRIMARY_KEYS.get(table_name, [])
    if not pk_cols:
        return results

    target = sql.Identifier('silver', table_name)
    pk_ids = sql.SQL(', ').join(sql.Identifier(col) for col in pk_cols)

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                SELECT COUNT(*) - COUNT(DISTINCT ({pk}))
                FROM {table}
                WHERE _snapshot_id = %s
                """
            ).format(pk = pk_ids, table = target), (snapshot_id,),
        )
        dupes = cur.fetchone()[0]
    passed = dupes == 0
    pk_label = '_'.join(pk_cols)
    results.append(QualityResult(
        table = table_name,
        check_name = f"pk_unique_{pk_label}",
        passed = passed,
        severity = 'error',
        details = {'pk_cols': pk_cols, 'duplicate_rows': dupes},
    ))
    if not passed:
        logger.warning('dq_silver_pk_duplicates', extra = {
            'table': table_name,
            'pk': pk_cols,
            'duplicate_rows': dupes,
        })
    return results

def check_cast_nulls(
    conn: extensions.connection, 
    table_name: str, 
    snapshot_id: str,
    effective_snapshot_id: str) -> list[QualityResult]:
    
    """
    Detect NULLs introduced by safe-cast.
    """
    columns = CAST_COLUMNS.get(table_name)
    if not columns:
        return []
    results = []
    pk_col = PRIMARY_KEYS[table_name][0]
    for silver_col, bronze_col in columns.items():
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
                    SELECT COUNT(*) FROM {silver_t} AS s
                    JOIN {bronze_t} AS b 
                    ON s.{pk} = b.{pk}
                    WHERE s._snapshot_id = %s
                      AND b._snapshot_id = %s
                      AND b.{b_col} IS NOT NULL
                      AND TRIM(b.{b_col}) != ''
                      AND s.{s_col} IS NULL
                    """
                ).format(
                    silver_t=sql.Identifier("silver", table_name),
                    bronze_t=sql.Identifier("bronze", table_name),
                    pk=sql.Identifier(pk_col),
                    b_col=sql.Identifier(bronze_col),
                    s_col=sql.Identifier(silver_col),
                ),
                (snapshot_id, effective_snapshot_id),
            )
            failures = cur.fetchone()[0]
        passed = failures == 0
        results.append(QualityResult(
            table = table_name, 
            check_name = f"cast_nulls_{silver_col}",
            passed = passed, 
            severity = 'warning',
            details = {'column': silver_col, 'bronze_column': bronze_col, 'cast_failures': failures},
        ))
        if not passed:
            logger.warning('dq_silver_cast_nulls', extra={
                'table': table_name, 'column': silver_col, 'failures': failures,
            })
    return results


### cross-table quality checks

def check_referential_integrity(conn: extensions.connection, snapshot_id: str) -> list[QualityResult]:
    """
    Check for referential integrity violations.
    """
    results = []
    for child_table, child_col, parent_table, parent_col in REFERENTIAL_CHECKS:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
                    SELECT COUNT(*)
                    FROM {child} AS c
                    LEFT JOIN {parent} AS p
                    ON c.{c_col} = p.{p_col} AND p._snapshot_id = %s
                    WHERE c._snapshot_id = %s AND p.{p_col} IS NULL
                    """
                ).format(
                    child = sql.Identifier('silver', child_table),
                    parent = sql.Identifier('silver', parent_table),
                    c_col = sql.Identifier(child_col),
                    p_col = sql.Identifier(parent_col),
                ), (snapshot_id, snapshot_id),
            )
            orphans = cur.fetchone()[0]

        passed = orphans == 0
        check_name = f"ref_integrity_{child_table}_{child_col}_to_{parent_table}"
        severity = 'error' if child_table == 'order_items' else 'warning'

        results.append(QualityResult(
            table = child_table, check_name = check_name, passed = passed, severity = severity,
            details = {'orphan_count': orphans, 'parent_table': parent_table, 'parent_col': parent_col},
        ))
        if not passed: 
            logger.warning('dq_silver_ref_integrity_violation', extra={
                'child': child_table, 'parent': parent_table, 'orphans': orphans,
            })
    return results


def check_range(
    conn: extensions.connection, snapshot_id: str
) -> list[QualityResult]:
    checks = [
        ('order_items',    'price',                'price >= 0',                   'error'),
        ('order_items',    'freight_value',        'freight_value >= 0',           'error'),
        ('order_payments', 'payment_value',        'payment_value >= 0',           'error'),
        ('order_payments', 'payment_installments', 'payment_installments >= 0',    'warning'),
        ('order_reviews',  'review_score',         'review_score BETWEEN 1 AND 5', 'error'),
    ]
    results = []
    for table, col, condition, severity in checks:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
                    SELECT COUNT(*) 
                    FROM {} 
                    WHERE _snapshot_id = %s AND NOT ({})
                    """
                ).format(
                    sql.Identifier('silver', table),
                    sql.SQL(condition),
                ),
                (snapshot_id,),
            )
            violations = cur.fetchone()[0]

        passed = violations == 0
        results.append(QualityResult(
            table = table, 
            check_name = f"range_{col}",
            passed = passed, 
            severity = severity,
            details = {'condition': condition, 'violations': violations},
        ))
        if not passed:
            logger.warning('dq_silver_range', extra={
                'table': table, 'column': col, 'violations': violations,
            })
    return results

def check_timestamp_order(conn:extensions.connection, snapshot_id:str) -> QualityResult:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM silver.orders
            WHERE _snapshot_id = %s
              AND (
                  (order_approved_at IS NOT NULL AND order_purchase_ts > order_approved_at)
               OR (delivered_carrier_at IS NOT NULL AND order_approved_at IS NOT NULL
                   AND order_approved_at > delivered_carrier_at)
               OR (delivered_customer_at IS NOT NULL AND delivered_carrier_at IS NOT NULL
                   AND delivered_carrier_at > delivered_customer_at)
            ) 
            """, (snapshot_id,),
        )
        violations = cur.fetchone()[0] 

    passed = violations == 0
    return QualityResult(
        table = 'orders', 
        check_name = 'timestamp_order',
        passed = passed,
        severity = 'warning',
        details = {'violations': violations},
    )

### run all quality checks

def run_quality_checks(conn: extensions.connection, table_name: str, 
    snapshot_id: str, effective_snapshot_id: str) -> list[QualityResult]:
    """
    Run all per-table quality checks for silver layer.
    """
    results = []
    results.append(check_not_empty(conn, table_name, snapshot_id))
    results.extend(check_pk_nulls(conn, table_name, snapshot_id))
    results.extend(check_pk_unique(conn, table_name, snapshot_id))
    results.extend(check_cast_nulls(conn, table_name, snapshot_id, effective_snapshot_id))
    return results

def run_cross_table_checks(conn: extensions.connection, snapshot_id: str) -> list[QualityResult]:
    """
    Run cross-table quality checks for silver layer. Must be called after all tables are loaded.
    """
    results = []
    results.extend(check_referential_integrity(conn, snapshot_id))
    results.extend(check_range(conn, snapshot_id))
    results.append(check_timestamp_order(conn, snapshot_id))
    return results

def persist_quality_results(conn:extensions.connection, run_id:str, results: list[QualityResult]):
    with conn.cursor() as cur:
        for res in results:
            cur.execute(
                """
                INSERT INTO ingestion.quality_checks (run_id, table_name, check_name, passed, details)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (run_id, table_name, check_name) DO UPDATE
                SET passed = EXCLUDED.passed, details = EXCLUDED.details, checked_at = NOW()
                """,
                (run_id, res.table, res.check_name, res.passed, json.dumps(res.details)),
            )
    conn.commit()