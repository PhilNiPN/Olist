"""
Tests for quality_bronze.py: the data quality checks that validates 
the bronze layer data after loading.

WHAT THESE TESTS COVER:
  check_row_count       — Actual vs. manifest row count (severity=error),
                          plus the skip path when manifest has no count (severity=warning).
  check_not_empty       — Table must have >= 1 row for the snapshot.
  check_primary_key_nulls — NULL rate on each PK column must be 0.
  check_schema          — PK + metadata columns must exist in information_schema.
  PrimaryKeyConsistency — Cross-check: every ALLOWED_TABLE has an entry in PRIMARY_KEYS.

WHY UNIT TESTS ARE ENOUGH:
  Every check does a single SELECT and returns a QualityResult dataclass.
  We control the SELECT output via mock_conn (fetchone / fetchall), so we can
  exercise pass, fail, and edge-case branches without a running database.

TECHNIQUE:
  mock_conn from conftest.py stubs conn.cursor() as a context manager.
  Each test configures the return value (fetchone or fetchall) and asserts
  the resulting QualityResult fields: passed, severity, and details.
"""

import pytest
from bronze.quality_bronze import (
    check_row_count,
    check_not_empty,
    check_primary_key_nulls,
    check_schema,
    PRIMARY_KEYS,
)
from db import ALLOWED_TABLES

### row count checks

class TestCheckRowCount:
    """
    WHAT: Verify check_row_count compares actual COUNT(*) against expected_rows.
    
    WHY:  A mismatch means the COPY loaded fewer/more rows than the CSV had,
          which could indicate truncation, duplication, or a corrupt file.
    
    TECHNIQUE: mock_conn(fetchone=...) sets the COUNT(*) result; expected_rows is passed directly.
    """
    def test_pass_when_counts_match(self, mock_conn):
        conn, _ = mock_conn(fetchone=(100,))
        result = check_row_count(conn, 'orders', 'snap1', expected_rows=100)
        assert result.passed is True
        assert result.details['expected'] == 100
        assert result.details['actual'] == 100
        assert result.severity == 'error'

    def test_fail_when_counts_differ(self, mock_conn):
        conn, _ = mock_conn(fetchone=(99,))
        result = check_row_count(conn, 'orders', 'snap1', expected_rows=100)
        assert result.passed is False
        assert result.details['actual'] == 99
        assert result.severity == 'error'

    def test_skip_when_no_expected_rows(self, mock_conn):
        conn, _ = mock_conn(fetchone=None)
        result = check_row_count(conn, 'orders', 'snap1', expected_rows=None)
        assert result.passed is True
        assert result.details['skipped'] is True
        assert result.severity == 'warning'


### not empty checks

class TestCheckNotEmpty:
    """
    WHAT: Verify check_not_empty flags tables with zero rows.
    
    WHY:  An empty bronze table after loading means the source file was empty
          or the COPY silently failed.  This is a severity=error gate.
    
    TECHNIQUE: mock_conn(fetchone=(N,)) where N is the row count.
    """

    def test_pass_when_rows_exist(self, mock_conn):
        conn, _ = mock_conn(fetchone=(42,))
        result = check_not_empty(conn, 'orders', 'snap1')
        assert result.passed is True
        assert result.severity == 'error'

    def test_fail_when_no_rows(self, mock_conn):
        conn, _ = mock_conn(fetchone=(0,))
        result = check_not_empty(conn, 'orders', 'snap1')
        assert result.passed is False
        assert result.details['row_count'] == 0
        assert result.severity == 'error'


### primary key nulls checks

class TestCheckPrimaryKeyNulls:
    """
    WHAT: Verify check_primary_key_nulls detects NULLs in primary-key columns.
    
    WHY:  PK NULLs break downstream JOINs in the silver layer and violate
          referential integrity assumptions.
    
    TECHNIQUE: mock_conn(fetchone=(total, null_count)) simulates the
    COUNT(*) / COUNT(*) FILTER (WHERE col IS NULL) query.
    """

    def test_pass_when_no_nulls(self, mock_conn):
        conn, _ = mock_conn(fetchone=(100,0))
        result = check_primary_key_nulls(conn, 'orders', 'snap1')
        assert len(result) == 1
        assert result[0].passed is True
        assert result[0].details['null_rate'] == 0
        assert result[0].severity == 'error'

    def test_fail_when_nulls_exist(self, mock_conn):
        conn, _ = mock_conn(fetchone=(200,10))
        result = check_primary_key_nulls(conn, 'orders', 'snap1')
        assert result[0].passed is False
        assert result[0].details['null_count'] == 10
        assert result[0].details['null_rate'] == 0.05
        assert result[0].severity == 'error'

    def test_handle_composite_pk(self, mock_conn):
        # order_items has composite PK: [order_id, order_item_id]
        conn, _ = mock_conn(fetchone=(50, 0))
        result = check_primary_key_nulls(conn, 'order_items', 'snap1')
        assert len(result) == 2
        assert all(r.passed for r in result)
        assert all(r.severity == 'error' for r in result)

    def test_returns_empty_for_unknown_table(self, mock_conn):
        conn, _ = mock_conn()
        result = check_primary_key_nulls(conn, 'non_existent_table', 'snap1')
        assert result == []


### schema checks

class TestCheckSchema:
    """
    WHAT: Verify check_schema detects missing PK and metadata columns.
    
    WHY:  If a bronze table is missing _snapshot_id or a PK column, downstream
          transforms and quality checks will fail with cryptic SQL errors.
    
    TECHNIQUE: mock_conn(fetchall=...) returns the column list from information_schema.
    """
    def test_pass_when_all_cols_present(self, mock_conn):
        required = (
            PRIMARY_KEYS['orders']
            + ['_snapshot_id', '_run_id', '_inserted_at', '_source_file']
            + ['order_status', 'customer_id']
        )
        conn, _ = mock_conn(fetchall=[(c,) for c in required])
        result = check_schema(conn, 'orders')
        assert result.passed is True
        assert result.details['missing_columns'] == []
        assert result.severity == 'error'

    def test_fail_when_metadata_col_missing(self, mock_conn):
        conn, _ = mock_conn(fetchall=[('order_id',), ('order_status',)])
        result = check_schema(conn, 'orders')
        assert result.passed is False
        assert '_snapshot_id' in result.details['missing_columns']
        assert result.severity == 'error'

    def test_fail_when_pk_col_missing(self, mock_conn):
        metadata = ['_snapshot_id', '_run_id', '_inserted_at', '_source_file']
        conn, _ = mock_conn(fetchall=[(c,) for c in metadata])
        result = check_schema(conn, 'orders')
        assert result.passed is False
        assert 'order_id' in result.details['missing_columns']
        assert result.severity == 'error'


### cross-table consistency checks

class TestPrimaryKeyConsistency:
    """
    PK in quality_bronze.py must match every table in db.ALLOWED_TABLES.

    "WHAT: Cross-check that PRIMARY_KEYS covers exactly the set of ALLOWED_TABLES.

    WHY:  If a new table is added to ALLOWED_TABLES but not to PRIMARY_KEYS,
          check_primary_key_nulls silently returns [] for it — no protection.
    """

    def test_every_allowed_table_has_pk(self):
        assert set(PRIMARY_KEYS.keys()) == ALLOWED_TABLES