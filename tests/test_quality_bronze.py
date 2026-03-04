"""
Tests for quality_bronze.py: the data quality checks that validates the bronze layer data after loading.
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
    def test_pass_when_counts_match(self, mock_conn):
        conn, _ = mock_conn(fetchone=(100,))
        result = check_row_count(conn, 'orders', 'snap1', expected_rows=100)
        assert result.passed is True
        assert result.details['expected'] == 100
        assert result.details['actual'] == 100

    def test_fail_when_counts_differ(self, mock_conn):
        conn, _ = mock_conn(fetchone=(99,))
        result = check_row_count(conn, 'orders', 'snap1', expected_rows=100)
        assert result.passed is False
        assert result.details['actual'] == 99

    def test_skip_when_no_expected_rows(self, mock_conn):
        conn, _ = mock_conn(fetchone=None)
        result = check_row_count(conn, 'orders', 'snap1', expected_rows=None)
        assert result.passed is True
        assert result.details['skipped'] is True


### not empty checks

class TestCheckNotEmpty:

    def test_pass_when_rows_exist(self, mock_conn):
        conn, _ = mock_conn(fetchone=(42,))
        result = check_not_empty(conn, 'orders', 'snap1')
        assert result.passed is True

    def test_fail_when_no_rows(self, mock_conn):
        conn, _ = mock_conn(fetchone=(0,))
        result = check_not_empty(conn, 'orders', 'snap1')
        assert result.passed is False
        assert result.details['row_count'] == 0


### primary key nulls checks

class TestCheckPrimaryKeyNulls:

    def test_pass_when_no_nulls(self, mock_conn):
        conn, _ = mock_conn(fetchone=(100,0))
        result = check_primary_key_nulls(conn, 'orders', 'snap1')
        assert len(result) == 1
        assert result[0].passed is True
        assert result[0].details['null_rate'] == 0

    def test_fail_when_nulls_exist(self, mock_conn):
        conn, _ = mock_conn(fetchone=(200,10))
        result = check_primary_key_nulls(conn, 'orders', 'snap1')
        assert result[0].passed is False
        assert result[0].details['null_count'] == 10
        assert result[0].details['null_rate'] == 0.05

    def test_handle_composite_pk(self, mock_conn):
        # order_items has composite PK: [order_id, order_item_id]
        conn, _ = mock_conn(fetchone=(50, 0))
        result = check_primary_key_nulls(conn, 'order_items', 'snap1')
        assert len(result) == 2
        assert all(r.passed for r in result)

    def test_returns_empty_for_unknown_table(self, mock_conn):
        conn, _ = mock_conn()
        result = check_primary_key_nulls(conn, 'non_existent_table', 'snap1')
        assert result == []


### schema checks

class TestCheckSchema:
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

    def test_fail_when_metadata_col_missing(self, mock_conn):
        conn, _ = mock_conn(fetchall=[('order_id',), ('order_status',)])
        result = check_schema(conn, 'orders')
        assert result.passed is False
        assert '_snapshot_id' in result.details['missing_columns']

    def test_fail_when_pk_col_missing(self, mock_conn):
        metadata = ['_snapshot_id', '_run_id', '_inserted_at', '_source_file']
        conn, _ = mock_conn(fetchall=[(c,) for c in metadata])
        result = check_schema(conn, 'orders')
        assert result.passed is False
        assert 'order_id' in result.details['missing_columns']


### cross-table consistency checks

class TestPrimaryKeyConsistency:
    """
    PK in quality_bronze.py must match every table in db.ALLOWED_TABLES.
    """

    def test_every_allowed_table_has_pk(self):
        assert set(PRIMARY_KEYS.keys()) == ALLOWED_TABLES