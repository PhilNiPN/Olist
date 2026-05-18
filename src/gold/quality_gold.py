"""
Quality checks for the gold layer.

Five categories of checks:
  1. Structural: PKs not null, PKs unique, tables not empty.
  2. Range: derived/carried columns stay within valid bounds.
  3. Transform NULLs: detect NULLs introduced by gold joins or computations
     that shouldn't be NULL given their silver source values.
  4. Row count vs silver: per dimension AND per fact — catch broken WHERE
     clauses or join mismatches early.
  5. Referential integrity: every FK in the fact resolves to a dimension row.

Uses the same QualityResult dataclass and persist pattern as silver.quality_silver
so results land in the shared ingestion.quality_checks table.
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
    severity: str      # 'error' = blocks load, 'warning' = logged but allowed
    details: dict


# Primary keys for each gold table.
# dim_dates is excluded because it uses ON CONFLICT and is loaded
# separately from the snapshot-scoped idempotency pattern.
PRIMARY_KEYS = {
    'dim_customers':    ['customer_unique_id'],
    'dim_products':     ['product_id'],
    'dim_sellers':      ['seller_id'],
    'fact_order_items': ['order_id', 'order_item_id'],
}

# Referential integrity: (child_table, child_col, parent_table, parent_col).
# Every FK in the fact should point to an existing dimension row.
REFERENTIAL_CHECKS = [
    ('fact_order_items', 'customer_unique_id', 'dim_customers', 'customer_unique_id'),
    ('fact_order_items', 'product_id',         'dim_products',  'product_id'),
    ('fact_order_items', 'seller_id',          'dim_sellers',   'seller_id'),
    ('fact_order_items', 'order_purchase_date','dim_dates',     'date_key'),
]


# ── per-table checks ────────────────────────────────────────────────

def check_not_empty(conn: extensions.connection, table_name: str, snapshot_id: str) -> QualityResult:
    """Fail if the table has zero rows for this snapshot."""
    target = sql.Identifier('gold', table_name)
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT COUNT(*) FROM {} WHERE _snapshot_id = %s").format(target),
            (snapshot_id,),
        )
        count = cur.fetchone()[0]

    passed = count > 0
    if not passed:
        logger.error('dq_gold_table_empty', extra={'table': table_name})
    return QualityResult(
        table=table_name, check_name='not_empty',
        passed=passed, severity='error',
        details={'row_count': count},
    )


def check_pk_nulls(conn: extensions.connection, table_name: str, snapshot_id: str) -> list[QualityResult]:
    """Every PK column must be fully populated — NULLs indicate a broken join."""
    results = []
    pk_cols = PRIMARY_KEYS.get(table_name, [])
    target = sql.Identifier('gold', table_name)

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
                ).format(sql.Identifier(col), target),
                (snapshot_id,),
            )
            total, null_count = cur.fetchone()

        passed = null_count == 0
        results.append(QualityResult(
            table=table_name, check_name=f"pk_null_{col}",
            passed=passed, severity='error',
            details={'column': col, 'total': total, 'null_count': null_count},
        ))
        if not passed:
            logger.warning('dq_gold_pk_nulls', extra={
                'table': table_name, 'column': col, 'null_count': null_count,
            })
    return results


def check_pk_unique(conn: extensions.connection, table_name: str, snapshot_id: str) -> list[QualityResult]:
    """PK must be unique — duplicates mean the join fan-out is wrong."""
    results = []
    pk_cols = PRIMARY_KEYS.get(table_name, [])
    if not pk_cols:
        return results

    target = sql.Identifier('gold', table_name)
    pk_ids = sql.SQL(', ').join(sql.Identifier(col) for col in pk_cols)

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                SELECT COUNT(*) - COUNT(DISTINCT ({pk}))
                FROM {table}
                WHERE _snapshot_id = %s
                """
            ).format(pk=pk_ids, table=target),
            (snapshot_id,),
        )
        dupes = cur.fetchone()[0]

    passed = dupes == 0
    pk_label = '_'.join(pk_cols)
    results.append(QualityResult(
        table=table_name, check_name=f"pk_unique_{pk_label}",
        passed=passed, severity='error',
        details={'pk_cols': pk_cols, 'duplicate_rows': dupes},
    ))
    if not passed:
        logger.warning('dq_gold_pk_duplicates', extra={
            'table': table_name, 'pk': pk_cols, 'duplicate_rows': dupes,
        })
    return results


# ── range checks ─────────────────────────────────────────────────────

# (table, column, SQL condition, severity)
# Silver validates source columns (price, review_score, etc.), but gold
# recomputes total_value and days_to_deliver — if the SQL arithmetic is
# wrong, silver's checks won't catch it.
RANGE_CHECKS = [
    ('fact_order_items', 'total_value',           'total_value >= 0',              'error'),
    ('fact_order_items', 'price',                 'price >= 0',                    'error'),
    ('fact_order_items', 'freight_value',         'freight_value >= 0',            'error'),
    ('fact_order_items', 'days_to_deliver',       'days_to_deliver >= 0',          'warning'),
    ('fact_order_items', 'review_score',          'review_score BETWEEN 1 AND 5',  'error'),
    ('fact_order_items', 'payment_installments',  'payment_installments >= 0',     'warning'),
]


def check_range(conn: extensions.connection, snapshot_id: str) -> list[QualityResult]:
    """
    Validate that derived and carried columns stay within expected bounds.
    Nullable columns (review_score, days_to_deliver, payment_installments)
    only check non-NULL values — NULL is valid (no review, not delivered, etc.).
    """
    results = []
    for table, col, condition, severity in RANGE_CHECKS:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
                    SELECT COUNT(*)
                    FROM {}
                    WHERE _snapshot_id = %s
                      AND {} IS NOT NULL
                      AND NOT ({})
                    """
                ).format(
                    sql.Identifier('gold', table),
                    sql.Identifier(col),
                    sql.SQL(condition),
                ),
                (snapshot_id,),
            )
            violations = cur.fetchone()[0]

        passed = violations == 0
        results.append(QualityResult(
            table=table, check_name=f"range_{col}",
            passed=passed, severity=severity,
            details={'condition': condition, 'violations': violations},
        ))
        if not passed:
            logger.warning('dq_gold_range', extra={
                'table': table, 'column': col, 'violations': violations,
            })
    return results


# ── row count vs silver (per table) ─────────────────────────────────

# (gold_table, silver_schema, silver_table, min_ratio)
# dim_customers deduplicates customer_unique_id → expect fewer rows than
# silver.customers (which has one row per customer_id). A ratio of ~0.40
# is normal for this dataset. Other dims are 1:1 passthroughs.
ROW_COUNT_CHECKS = [
    ('dim_customers', 'silver', 'customers', 0.40),
    ('dim_products',  'silver', 'products',  1.0),
    ('dim_sellers',   'silver', 'sellers',   1.0),
    ('fact_order_items', 'silver', 'order_items', 0.95),
]


def check_row_count_vs_silver(conn: extensions.connection, snapshot_id: str) -> list[QualityResult]:
    """
    Compare gold row counts against their silver source tables.
    A ratio below the threshold means the gold transform lost rows
    (broken WHERE, failed join, etc.).
    """
    results = []
    for gold_table, silver_schema, silver_table, min_ratio in ROW_COUNT_CHECKS:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("SELECT COUNT(*) FROM {} WHERE _snapshot_id = %s").format(
                    sql.Identifier('gold', gold_table)
                ), (snapshot_id,),
            )
            gold_count = cur.fetchone()[0]

            cur.execute(
                sql.SQL("SELECT COUNT(*) FROM {} WHERE _snapshot_id = %s").format(
                    sql.Identifier(silver_schema, silver_table)
                ), (snapshot_id,),
            )
            silver_count = cur.fetchone()[0]

        ratio = gold_count / silver_count if silver_count > 0 else 0.0
        passed = ratio >= min_ratio

        results.append(QualityResult(
            table=gold_table,
            check_name=f"row_count_vs_silver_{silver_table}",
            passed=passed,
            severity='warning',
            details={
                'gold_rows': gold_count,
                'silver_rows': silver_count,
                'ratio': round(ratio, 4),
                'min_ratio': min_ratio,
            },
        ))
        if not passed:
            logger.warning('dq_gold_row_count_mismatch', extra={
                'gold_table': gold_table, 'silver_table': silver_table,
                'ratio': round(ratio, 4), 'min_ratio': min_ratio,
            })
    return results


# ── transform NULL detection ─────────────────────────────────────────

# Each entry: (description of what we're checking,
#   gold_table, gold_col, silver_table, silver_join_col, gold_join_col,
#   silver_condition_for_non_null)
#
# The idea: if a silver source value is non-NULL, the gold derived value
# should also be non-NULL. A mismatch means the gold join or computation
# introduced an unexpected NULL.
TRANSFORM_NULL_CHECKS = [
    # total_value = price + freight; if both inputs exist, output must exist
    {
        'gold_table': 'fact_order_items',
        'gold_col': 'total_value',
        'silver_table': 'order_items',
        'join': 'g.order_id = s.order_id AND g.order_item_id = s.order_item_id',
        'silver_non_null': 's.price IS NOT NULL AND s.freight_value IS NOT NULL',
    },
    # order_purchase_date is cast from silver.orders.order_purchase_ts
    {
        'gold_table': 'fact_order_items',
        'gold_col': 'order_purchase_date',
        'silver_table': 'orders',
        'join': 'g.order_id = s.order_id',
        'silver_non_null': 's.order_purchase_ts IS NOT NULL',
    },
    # customer_unique_id comes from JOIN orders→customers; if the silver
    # order has a customer_id, gold must resolve the unique_id
    {
        'gold_table': 'fact_order_items',
        'gold_col': 'customer_unique_id',
        'silver_table': 'orders',
        'join': 'g.order_id = s.order_id',
        'silver_non_null': 's.customer_id IS NOT NULL',
    },
    # product_volume_cm3 = L*H*W; should be non-NULL when all three are non-NULL
    {
        'gold_table': 'dim_products',
        'gold_col': 'product_volume_cm3',
        'silver_table': 'products',
        'join': 'g.product_id = s.product_id',
        'silver_non_null': (
            's.product_length_cm IS NOT NULL '
            'AND s.product_height_cm IS NOT NULL '
            'AND s.product_width_cm IS NOT NULL'
        ),
    },
]


def check_transform_nulls(conn: extensions.connection, snapshot_id: str) -> list[QualityResult]:
    """
    Detect NULLs that were introduced by gold-layer joins or computations.
    For each check: count rows where the silver source is non-NULL but the
    gold output is NULL — these are unexpected and indicate a broken transform.
    """
    results = []
    for chk in TRANSFORM_NULL_CHECKS:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
                    SELECT COUNT(*)
                    FROM {gold} g
                    JOIN {silver} s ON {join_cond} AND s._snapshot_id = %s
                    WHERE g._snapshot_id = %s
                      AND ({silver_non_null})
                      AND g.{gold_col} IS NULL
                    """
                ).format(
                    gold=sql.Identifier('gold', chk['gold_table']),
                    silver=sql.Identifier('silver', chk['silver_table']),
                    join_cond=sql.SQL(chk['join']),
                    silver_non_null=sql.SQL(chk['silver_non_null']),
                    gold_col=sql.Identifier(chk['gold_col']),
                ),
                (snapshot_id, snapshot_id),
            )
            failures = cur.fetchone()[0]

        passed = failures == 0
        check_name = f"transform_null_{chk['gold_table']}_{chk['gold_col']}"
        results.append(QualityResult(
            table=chk['gold_table'], check_name=check_name,
            passed=passed, severity='warning',
            details={
                'gold_column': chk['gold_col'],
                'silver_table': chk['silver_table'],
                'unexpected_nulls': failures,
            },
        ))
        if not passed:
            logger.warning('dq_gold_transform_nulls', extra={
                'gold_table': chk['gold_table'],
                'gold_col': chk['gold_col'],
                'failures': failures,
            })
    return results


# ── cross-table checks ──────────────────────────────────────────────

def check_referential_integrity(conn: extensions.connection, snapshot_id: str) -> list[QualityResult]:
    """Every FK in the fact must resolve to a row in the referenced dimension."""
    results = []
    for child_table, child_col, parent_table, parent_col in REFERENTIAL_CHECKS:
        # dim_dates has no _snapshot_id column — it is append-only across snapshots
        if parent_table == 'dim_dates':
            query = sql.SQL(
                """
                SELECT COUNT(*)
                FROM {child} AS c
                LEFT JOIN {parent} AS p ON c.{c_col} = p.{p_col}
                WHERE c._snapshot_id = %s AND p.{p_col} IS NULL
                """
            ).format(
                child=sql.Identifier('gold', child_table),
                parent=sql.Identifier('gold', parent_table),
                c_col=sql.Identifier(child_col),
                p_col=sql.Identifier(parent_col),
            )
            params = (snapshot_id,)
        else:
            query = sql.SQL(
                """
                SELECT COUNT(*)
                FROM {child} AS c
                LEFT JOIN {parent} AS p
                    ON c.{c_col} = p.{p_col} AND p._snapshot_id = %s
                WHERE c._snapshot_id = %s AND p.{p_col} IS NULL
                """
            ).format(
                child=sql.Identifier('gold', child_table),
                parent=sql.Identifier('gold', parent_table),
                c_col=sql.Identifier(child_col),
                p_col=sql.Identifier(parent_col),
            )
            params = (snapshot_id, snapshot_id)

        with conn.cursor() as cur:
            cur.execute(query, params)
            orphans = cur.fetchone()[0]

        passed = orphans == 0
        results.append(QualityResult(
            table=child_table,
            check_name=f"ref_integrity_{child_col}_to_{parent_table}",
            passed=passed, severity='error',
            details={'orphan_count': orphans, 'parent_table': parent_table, 'parent_col': parent_col},
        ))
        if not passed:
            logger.warning('dq_gold_ref_integrity_violation', extra={
                'child': child_table, 'parent': parent_table, 'orphans': orphans,
            })
    return results


# ── runners ─────────────────────────────────────────────────────────

def run_quality_checks(conn: extensions.connection, table_name: str, snapshot_id: str) -> list[QualityResult]:
    """Per-table structural checks: not-empty, PK nulls, PK uniqueness."""
    results = []

    # dim_dates has no _snapshot_id, so skip snapshot-scoped checks for it
    if table_name == 'dim_dates':
        return results

    results.append(check_not_empty(conn, table_name, snapshot_id))
    results.extend(check_pk_nulls(conn, table_name, snapshot_id))
    results.extend(check_pk_unique(conn, table_name, snapshot_id))
    return results


def run_cross_table_checks(conn: extensions.connection, snapshot_id: str) -> list[QualityResult]:
    """
    Cross-table checks run after all gold tables are loaded:
      - Referential integrity (fact FKs → dimensions)
      - Row count vs silver (per dimension + fact)
      - Range checks on derived columns
      - Transform NULL detection (joins/computations introducing unexpected NULLs)
    """
    results = []
    results.extend(check_referential_integrity(conn, snapshot_id))
    results.extend(check_row_count_vs_silver(conn, snapshot_id))
    results.extend(check_range(conn, snapshot_id))
    results.extend(check_transform_nulls(conn, snapshot_id))
    return results


def persist_quality_results(conn: extensions.connection, run_id: str, results: list[QualityResult]):
    """Write results to ingestion.quality_checks. No commit — caller decides."""
    with conn.cursor() as cur:
        for res in results:
            cur.execute(
                """
                INSERT INTO ingestion.quality_checks (run_id, table_name, check_name, passed, severity, details)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, table_name, check_name) DO UPDATE
                SET passed = EXCLUDED.passed, severity = EXCLUDED.severity,
                    details = EXCLUDED.details, checked_at = NOW()
                """,
                (run_id, res.table, res.check_name, res.passed, res.severity, json.dumps(res.details)),
            )
