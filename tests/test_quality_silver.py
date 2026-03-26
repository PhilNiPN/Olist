"""
Tests for quality_silver.py: the data quality checks that validate the silver layer data after loading.

PATTERN: Each DQ function gets its own test class.  Inside the class we test the
         "happy path" (check passes) AND at least one "sad path" (check fails).
         This proves the function correctly distinguishes good data from bad.

MOCKING: Every function under test takes a `conn` (psycopg2 connection).  We don't
         need a real DB — the `mock_conn` fixture from conftest.py returns a fake
         connection whose cursor returns whatever we tell it to.  The function
         builds SQL and calls cur.execute() + cur.fetchone(); we only care about
         what fetchone *returns*, because that's what drives the pass/fail logic.
"""

import pytest
from silver.quality_silver import (
    check_not_empty,
    check_pk_nulls,
    check_pk_unique,
    check_cast_nulls,
    check_row_count_vs_bronze,
    check_referential_integrity,
    check_range,
    check_timestamp_order,
    PRIMARY_KEYS,
    CAST_COLUMNS,
    REFERENTIAL_CHECKS,
)
from silver.config import SILVER_TABLE_SOURCES


### not empty checks

class TestCheckNotEmpty:
    """ check_not_empty runs SELECT COUNT(*) and fails when the result is 0. """

    def test_pass_when_rows_exist(self, mock_conn):
        # Simulate: the COUNT(*) query returns 42 rows
        conn, _ = mock_conn(fetchone=(42,))
        result = check_not_empty(conn, 'orders', 'snap1')
        assert result.passed is True
        assert result.details['row_count'] == 42

    def test_fail_when_zero_rows(self, mock_conn):
        # Simulate: the table is empty — COUNT(*) returns 0
        conn, _ = mock_conn(fetchone=(0,))
        result = check_not_empty(conn, 'orders', 'snap1')
        assert result.passed is False
        assert result.details['row_count'] == 0


### primary key null checks

class TestCheckPkNulls:
    """
    check_pk_nulls returns one QualityResult *per PK column*.
    The query returns (total_rows, null_count).  passed = (null_count == 0).
    """

    def test_pass_when_no_nulls(self, mock_conn):
        # (100 total rows, 0 nulls) -> passes
        conn, _ = mock_conn(fetchone=(100, 0))
        result = check_pk_nulls(conn, 'orders', 'snap1')
        # orders has a single PK column -> expect 1 result
        assert len(result) == 1
        assert result[0].passed is True
        assert result[0].details['null_count'] == 0

    def test_fail_when_nulls_exist(self, mock_conn):
        # (200 total, 5 nulls) -> fails
        conn, _ = mock_conn(fetchone=(200, 5))
        result = check_pk_nulls(conn, 'orders', 'snap1')
        assert result[0].passed is False
        assert result[0].details['null_count'] == 5

    def test_composite_pk_order_items(self, mock_conn):
        # order_items has a composite PK: [order_id, order_item_id].
        # The function loops over each column, so we get 2 QualityResults.
        # mock_conn returns the SAME fetchone for every call, so both pass.
        conn, _ = mock_conn(fetchone=(50, 0))
        result = check_pk_nulls(conn, 'order_items', 'snap1')
        assert len(result) == 2
        assert all(r.passed for r in result)
        # Verify the check_name encodes which column was checked
        assert result[0].check_name == 'pk_null_order_id'
        assert result[1].check_name == 'pk_null_order_item_id'


### primary key uniqueness checks

class TestCheckPkUnique:
    """
    check_pk_unique counts duplicates: COUNT(*) - COUNT(DISTINCT pk).
    Returned as a single-element list.  0 duplicates -> passes.
    """

    def test_pass_when_no_dupes(self, mock_conn):
        # COUNT(*) - COUNT(DISTINCT pk) = 0 -> no duplicates
        conn, _ = mock_conn(fetchone=(0,))
        result = check_pk_unique(conn, 'orders', 'snap1')
        assert len(result) == 1
        assert result[0].passed is True
        assert result[0].details['duplicate_rows'] == 0

    def test_fail_when_dupes_exist(self, mock_conn):
        # 3 duplicate rows detected
        conn, _ = mock_conn(fetchone=(3,))
        result = check_pk_unique(conn, 'orders', 'snap1')
        assert result[0].passed is False
        assert result[0].details['duplicate_rows'] == 3


### cast null checks (unique to silver)

class TestCheckCastNulls:
    """
    check_cast_nulls detects NULLs introduced by safe-cast functions.
    It joins silver ↔ bronze: if bronze had a non-empty value but silver is NULL,
    that means the cast failed.

    This check only runs for tables listed in CAST_COLUMNS; others return [].
    It calls fetchone once per cast column, so we use cursor.fetchone.side_effect
    to provide different return values for each call.
    """

    def test_pass_no_cast_failures(self, mock_conn):
        # Every cast column returns 0 failures
        conn, cur = mock_conn(fetchone=(0,))
        result = check_cast_nulls(conn, 'geolocation', 'snap1', 'eff_snap1')
        # geolocation has 2 cast columns (lat, lng) -> 2 results, both passing
        assert len(result) == len(CAST_COLUMNS['geolocation'])
        assert all(r.passed for r in result)

    def test_fail_cast_failures_detected(self, mock_conn):
        conn, cur = mock_conn(fetchone=(0,))
        # Override: first column has 0 failures, second has 7.
        # side_effect makes fetchone return each value in sequence.
        cur.fetchone.side_effect = [(0,), (7,)]
        result = check_cast_nulls(conn, 'geolocation', 'snap1', 'eff_snap1')
        assert result[0].passed is True
        assert result[1].passed is False
        assert result[1].details['cast_failures'] == 7

    def test_skip_table_not_in_cast_columns(self, mock_conn):
        # 'sellers' has no cast columns -> function returns [] immediately
        conn, _ = mock_conn()
        result = check_cast_nulls(conn, 'sellers', 'snap1', 'eff_snap1')
        assert result == []


### row count vs bronze checks

class TestCheckRowCountVsBronze:
    """
    check_row_count_vs_bronze calls fetchone TWICE in the same cursor:
    first for the silver count, then for the bronze count.

    TECHNIQUE: We set cursor.fetchone.side_effect to a list of return values.
    Each call pops the next value from the list:
        side_effect = [(silver_count,), (bronze_count,)]
    """

    def test_pass_at_one_to_one(self, mock_conn):
        conn, cur = mock_conn(fetchone=(100,))
        # silver=100, bronze=100 -> ratio 1.0 -> passes (threshold is 1.0)
        cur.fetchone.side_effect = [(100,), (100,)]
        result = check_row_count_vs_bronze(conn, 'orders', 'snap1', 'eff_snap1')
        assert result.passed is True
        assert result.details['ratio'] == 1.0

    def test_fail_when_silver_below_bronze(self, mock_conn):
        conn, cur = mock_conn(fetchone=(50,))
        # silver=50, bronze=100 -> ratio 0.5 -> fails for non-geolocation (threshold 1.0)
        cur.fetchone.side_effect = [(50,), (100,)]
        result = check_row_count_vs_bronze(conn, 'orders', 'snap1', 'eff_snap1')
        assert result.passed is False
        assert result.details['silver_rows'] == 50
        assert result.details['bronze_rows'] == 100

    def test_geolocation_allows_lower_ratio(self, mock_conn):
        conn, cur = mock_conn(fetchone=(60,))
        # silver=60, bronze=100 -> ratio 0.6 -> passes for geolocation (threshold 0.5)
        # because geolocation deduplicates by zip code, losing rows is expected.
        cur.fetchone.side_effect = [(60,), (100,)]
        result = check_row_count_vs_bronze(conn, 'geolocation', 'snap1', 'eff_snap1')
        assert result.passed is True
        assert result.details['ratio'] == 0.6


### referential integrity checks

class TestCheckReferentialIntegrity:
    """
    check_referential_integrity loops over REFERENTIAL_CHECKS (6 FK relationships).
    For each, it LEFT JOINs child -> parent and counts orphans (parent PK is NULL).

    TECHNIQUE: Use side_effect with a list of len(REFERENTIAL_CHECKS) values.
    Each call to fetchone consumes the next value in the list.
    """

    def test_pass_no_orphans(self, mock_conn):
        conn, cur = mock_conn(fetchone=(0,))
        # All 6 checks return 0 orphans
        cur.fetchone.side_effect = [(0,)] * len(REFERENTIAL_CHECKS)
        results = check_referential_integrity(conn, 'snap1')
        assert len(results) == len(REFERENTIAL_CHECKS)
        assert all(r.passed for r in results)

    def test_fail_orphans_exist(self, mock_conn):
        conn, cur = mock_conn(fetchone=(0,))
        # Make the first check (order_items -> orders) return 5 orphans
        side = [(0,)] * len(REFERENTIAL_CHECKS)
        side[0] = (5,)
        cur.fetchone.side_effect = side
        results = check_referential_integrity(conn, 'snap1')
        assert results[0].passed is False
        assert results[0].details['orphan_count'] == 5
        # All other FK checks still pass
        assert all(r.passed for r in results[1:])


### range checks

class TestCheckRange:
    """
    check_range validates 5 hardcoded business rules (e.g. price >= 0).
    Each rule runs a separate query. Same side_effect technique as above.
    """

    def test_pass_all_in_range(self, mock_conn):
        conn, cur = mock_conn(fetchone=(0,))
        # All 5 range checks find 0 violations
        cur.fetchone.side_effect = [(0,)] * 5
        results = check_range(conn, 'snap1')
        assert len(results) == 5
        assert all(r.passed for r in results)

    def test_fail_negative_price(self, mock_conn):
        conn, cur = mock_conn(fetchone=(0,))
        # First check is "price >= 0"; simulate 2 rows violating it
        side = [(0,)] * 5
        side[0] = (2,)
        cur.fetchone.side_effect = side
        results = check_range(conn, 'snap1')
        assert results[0].passed is False
        assert results[0].details['violations'] == 2
        assert results[0].check_name == 'range_price'


### timestamp order checks

class TestCheckTimestampOrder:
    """
    check_timestamp_order checks that order lifecycle timestamps are in
    chronological order: purchase < approved < carrier < customer.
    Single fetchone call -> simple mock.
    """

    def test_pass_correct_order(self, mock_conn):
        # 0 rows violating the ordering -> passes
        conn, _ = mock_conn(fetchone=(0,))
        result = check_timestamp_order(conn, 'snap1')
        assert result.passed is True

    def test_fail_out_of_order(self, mock_conn):
        # 3 rows where timestamps are out of order
        conn, _ = mock_conn(fetchone=(3,))
        result = check_timestamp_order(conn, 'snap1')
        assert result.passed is False
        assert result.details['violations'] == 3


### config consistency checks

class TestSilverPkConsistency:
    """
    Guard against config drift: if someone adds a table to SILVER_TABLE_SOURCES
    but forgets to add it to PRIMARY_KEYS (or vice versa), this test catches it.
    No mocking needed — just comparing two dicts' key sets.
    """

    def test_pk_keys_match_silver_table_sources(self):
        assert set(PRIMARY_KEYS.keys()) == set(SILVER_TABLE_SOURCES.keys())


class TestCastColumnsConsistency:
    """
    Every table in CAST_COLUMNS must also exist in PRIMARY_KEYS,
    because check_cast_nulls uses PRIMARY_KEYS[table][0] as the join key.
    An orphaned entry in CAST_COLUMNS would cause a KeyError at runtime.
    """

    def test_cast_columns_subset_of_primary_keys(self):
        assert set(CAST_COLUMNS.keys()).issubset(set(PRIMARY_KEYS.keys()))
