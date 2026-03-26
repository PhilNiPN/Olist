"""
Integration tests for the silver layer (requires a running Postgres instance via Docker).
Run with: pytest -m integration

WHAT MAKES THESE "INTEGRATION" TESTS:
  Unlike the unit tests (which use mock_conn and never touch a database), these tests
  run real SQL against a real Postgres instance.  They prove that the SQL transforms,
  safe-cast functions, quality checks, SAVEPOINT rollback, and resume logic all work
  together end-to-end — not just in isolation.

FIXTURE OVERVIEW (defined in conftest.py):
  - txn:          Wraps each test in a SAVEPOINT so it can roll back all changes after.
  - seed_bronze:  Inserts a minimal, self-consistent bronze dataset for one snapshot.
  - load_conn:    Patches load() to use our test connection with commits disabled.
  - mock_resolve: Bypasses the snapshot-resolution query so we control which snapshot is used.

The pytestmark below applies @pytest.mark.integration to EVERY test in this file,
so 'pytest -m integration' selects them and plain 'pytest' skips them.
"""

import uuid
from datetime import datetime

import pytest

pytestmark = pytest.mark.integration


def _build_effective(snapshot_id):
    """
    Helper: build an effective-snapshot mapping where every bronze table uses the same snapshot.
    In production, resolve_effective_snapshot queries the DB to figure out which bronze snapshot to read from.  
    In tests we control the snapshot, so we just map every table to it directly.
    """
    from silver.config import SILVER_TABLE_SOURCES
    all_bronze = set()
    for sources in SILVER_TABLE_SOURCES.values():
        all_bronze.update(sources)
    return {t: snapshot_id for t in all_bronze}


### Transform round-trip

def test_transform_round_trip(txn, seed_bronze):
    """
    WHAT: Insert known bronze rows -> run the orders SQL transform -> query silver.
    
    WHY:  Proves that safe_cast_timestamptz actually converts TEXT -> TIMESTAMPTZ in Postgres.
          A unit test with mock_conn can't verify this because the cast happens in the DB engine.

    APPROACH:
      1. seed_bronze() inserts one bronze.orders row with TEXT timestamps.
      2. We execute the orders.sql transform directly (no need for the full load() machinery).
      3. We SELECT from silver.orders and verify the values are Python datetime objects,
         which confirms Postgres successfully cast them from TEXT.
    """
    snap_id, _ = seed_bronze()
    run_id = str(uuid.uuid4())

    from silver.transform_silver import TRANSFORMS
    from silver.load_silver import _build_query_params

    # Build the parameter dict that the SQL template expects:
    # %(target_snapshot_id)s, %(run_id)s, %(eff_orders)s
    params = _build_query_params(snap_id, run_id, 'orders', _build_effective(snap_id))

    with txn.cursor() as cur:
        # Run the actual INSERT INTO silver.orders (SELECT ... FROM bronze.orders)
        cur.execute(TRANSFORMS['orders'], params)
        assert cur.rowcount == 1  # we seeded exactly 1 bronze row

        cur.execute(
            """
            SELECT order_id, order_purchase_ts, order_approved_at, delivered_carrier_at 
            FROM silver.orders WHERE _snapshot_id = %s
            """, (snap_id,),
        )
        row = cur.fetchone()

    # order_id is passed through as-is (TEXT -> TEXT)
    assert row[0] == 'ord_001'
    # These columns went through safe_cast_timestamptz.
    # If the cast worked, psycopg2 returns them as Python datetime objects.
    assert isinstance(row[1], datetime)
    assert isinstance(row[2], datetime)
    assert isinstance(row[3], datetime)


### Safe-cast detection

def test_safe_cast_detection(txn, seed_bronze):
    """
    WHAT: Put garbage in a timestamp column -> verify it becomes NULL -> verify DQ catches it.
    
    WHY:  This tests the two-step safety net:
          1. safe_cast_timestamptz returns NULL instead of crashing on bad data.
          2. check_cast_nulls detects the mismatch (bronze had a value, silver has NULL).

    APPROACH:
      1. Seed normal bronze data, then UPDATE one column to 'not_a_date'.
      2. Run the transform — it should succeed (not crash), with NULL in that column.
      3. Call check_cast_nulls and verify it found exactly 1 cast failure.
    """
    snap_id, _ = seed_bronze()
    run_id = str(uuid.uuid4())

    # Corrupt one column: order_approved_at is nullable, so the INSERT won't fail,
    # but safe_cast_timestamptz('not_a_date') will return NULL.
    with txn.cursor() as cur:
        cur.execute(
            """
            UPDATE bronze.orders 
            SET order_approved_at = 'not_a_date' 
            WHERE _snapshot_id = %s
            """, (snap_id,),
        )

    from silver.transform_silver import TRANSFORMS
    from silver.load_silver import _build_query_params

    params = _build_query_params(snap_id, run_id, 'orders', _build_effective(snap_id))

    with txn.cursor() as cur:
        cur.execute(TRANSFORMS['orders'], params)
        cur.execute(
            """
            SELECT order_approved_at 
            FROM silver.orders 
            WHERE _snapshot_id = %s
            """, (snap_id,),
        )
        # safe_cast returned NULL because 'not_a_date' is not a valid timestamp
        assert cur.fetchone()[0] is None

    from silver.quality_silver import check_cast_nulls

    # check_cast_nulls joins silver ↔ bronze: bronze had 'not_a_date' (non-null),
    # silver has NULL -> that's 1 cast failure.
    results = check_cast_nulls(txn, 'orders', snap_id, snap_id)
    approved = [r for r in results if r.details['column'] == 'order_approved_at'][0]
    assert approved.passed is False
    assert approved.details['cast_failures'] == 1


### DQ circuit breaker

def test_dq_circuit_breaker(load_conn, seed_bronze, mock_resolve):
    """
    WHAT: Omit bronze data for sellers -> load() -> verify sellers is rolled back + rejected.
    
    WHY:  Proves the SAVEPOINT circuit-breaker pattern in load():
          1. Data is inserted inside a SAVEPOINT.
          2. Quality checks run on the inserted data.
          3. If a check fails with severity='error', ROLLBACK TO SAVEPOINT undoes the insert.
          4. The rejection is recorded in silver_table_loads and quality_checks.

    APPROACH:
      seed_bronze(exclude_tables = {'sellers'}) inserts data for all tables EXCEPT sellers.
      When the sellers transform runs, it finds 0 bronze rows -> inserts 0 silver rows.
      check_not_empty fails (severity = error) -> circuit breaker fires.
    """
    snap_id, _ = seed_bronze(exclude_tables={'sellers'})
    mock_resolve(snap_id)

    from silver.load_silver import load

    summary = load(snapshot_id=snap_id)

    # At least one table was DQ-rejected
    assert summary.tables_rejected >= 1

    with load_conn.cursor() as cur:
        # Silver sellers table should be empty (the ROLLBACK TO SAVEPOINT undid the insert)
        cur.execute(
            """
            SELECT COUNT(*) 
            FROM silver.sellers 
            WHERE _snapshot_id = %s
            """, (snap_id,),
        )
        assert cur.fetchone()[0] == 0

        # The metadata table should record the rejection
        cur.execute(
            """
            SELECT status 
            FROM ingestion.silver_table_loads 
            WHERE run_id = %s AND silver_table = 'sellers'
            """, (summary.run_id,),
        )
        assert cur.fetchone()[0] == 'dq_rejected'

        # The quality check itself should be persisted (passed=False)
        cur.execute(
            """
            SELECT passed 
            FROM ingestion.quality_checks 
            WHERE run_id = %s AND table_name = 'sellers' AND check_name = 'not_empty'
            """, (summary.run_id,),
        )
        assert cur.fetchone()[0] is False


### Idempotency

def test_idempotency(load_conn, seed_bronze, mock_resolve):
    """
    WHAT: Run load() twice with the same snapshot -> row count should not change.
    
    WHY:  Proves the DELETE-before-INSERT pattern works:
          Each transform does DELETE FROM silver.X WHERE _snapshot_id = ... before inserting.
          Running twice should produce the exact same result, not double the rows.
    """
    snap_id, _ = seed_bronze()
    mock_resolve(snap_id)

    from silver.load_silver import load

    # First load
    load(snapshot_id=snap_id)

    with load_conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) 
            FROM silver.orders 
            WHERE _snapshot_id = %s
            """, (snap_id,)
        )
        count_first = cur.fetchone()[0]

    # Second load — same snapshot, different run_id (generated internally)
    load(snapshot_id=snap_id)

    with load_conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) 
            FROM silver.orders 
            WHERE _snapshot_id = %s
            """, (snap_id,)
        )
        count_second = cur.fetchone()[0]

    assert count_first > 0              # sanity check: we actually loaded something
    assert count_first == count_second  # idempotency: no extra rows


### Resume

def test_resume(load_conn, seed_bronze, mock_resolve, monkeypatch):
    """
    WHAT: First load with intentionally broken SQL for sellers -> resume -> all tables loaded.
    WHY:  Proves the resume feature:
          1. On first run, 7 tables succeed and sellers fails (bad SQL).
          2. _get_completed_tables returns those 7 as 'loaded'.
          3. On resume, only sellers is retried (now with fixed SQL).
          4. After resume, all 8 tables are loaded.

    TECHNIQUE:
      monkeypatch.setattr replaces the TRANSFORMS dict with a copy where sellers
      has invalid SQL.  After the first load, we mutate the dict back to the
      original SQL.  The second load(resume = True) re-reads the same dict object
      and sees the fixed SQL.
    """
    snap_id, _ = seed_bronze()
    mock_resolve(snap_id)
    run_id = str(uuid.uuid4())

    import silver.load_silver as load_mod

    # Save the real SQL, then replace it with something that will crash
    original_sellers_sql = load_mod.TRANSFORMS['sellers']
    patched = dict(load_mod.TRANSFORMS)
    patched['sellers'] = 'SELECT 1/0'  # division by zero -> guaranteed exception
    monkeypatch.setattr(load_mod, 'TRANSFORMS', patched)

    # First load: 7 succeed, sellers fails
    summary1 = load_mod.load(snapshot_id=snap_id, run_id=run_id)
    assert summary1.tables_failed == 1

    # Fix the SQL by mutating the same dict (monkeypatch still points to it)
    patched['sellers'] = original_sellers_sql

    # Resume: pass the SAME run_id so it finds the previous loaded tables
    summary2 = load_mod.load(snapshot_id=snap_id, run_id=run_id, resume=True)
    assert summary2.tables_failed == 0
    assert summary2.tables_loaded == 8  # 7 skipped (already loaded) + 1 retried

    # Verify sellers actually has data now
    with load_conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) 
            FROM silver.sellers 
            WHERE _snapshot_id = %s
            """, (snap_id,)
        )
        assert cur.fetchone()[0] == 1
