"""
Shared test fixtures used across all test modules.

FIXTURE TYPES:
  1. mock_conn         — For unit tests.  Creates a fake psycopg2 connection that
                         returns whatever data you configure (fetchone, fetchall).
  2. db_conn / txn     — For integration tests.  Connects to real Postgres and wraps
                         each test in a SAVEPOINT so it rolls back automatically.
  3. seed_bronze       — Inserts a minimal, referentially consistent bronze dataset.
  4. load_conn         — Patches load_silver's connection and health_check so we can
                         call load() against the test DB with commits disabled.
  5. mock_resolve      — Replaces resolve_effective_snapshot with a simple mapping.
"""

import os
import uuid
from contextlib import contextmanager

import pytest
from unittest.mock import MagicMock


@pytest.fixture
def mock_conn():
    """
    Returns a factory that creates mock psycopg2 connections.
    Supports conn.cursor() as a context manager, matching how all our modules use it.

    HOW IT WORKS:
      All our DB functions do:  with conn.cursor() as cur: cur.execute(...); cur.fetchone()
      This fixture creates a MagicMock that makes that pattern work:
        - conn.cursor() returns a context manager (__enter__/__exit__)
        - __enter__ returns our mock cursor
        - cursor.fetchone / cursor.fetchall return whatever you passed in

    USAGE:
      conn, cur = mock_conn(fetchone=(100,))        # single row
      conn, cur = mock_conn(fetchall=[('a',), ('b',)])  # multiple rows

    FOR MULTIPLE CALLS:
      When a function calls fetchone() more than once (e.g. check_row_count_vs_bronze),
      override with side_effect AFTER creating the mock:
        conn, cur = mock_conn(fetchone=(0,))
        cur.fetchone.side_effect = [(100,), (200,)]   # first call → 100, second → 200
    """
    def _factory(fetchone=None, fetchall=None):
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        if fetchone is not None:
            cursor.fetchone.return_value = fetchone
        if fetchall is not None:
            cursor.fetchall.return_value = fetchall
        return conn, cursor
    return _factory


### Integration test fixtures (require running Postgres via Docker)

@pytest.fixture(scope='session')
def db_conn():
    """Session-scoped real Postgres connection. Skips if DB is unavailable.

    WHY SESSION-SCOPED: Opening a DB connection is slow. We reuse one connection
    for the entire test session.  Individual tests are isolated via the txn fixture
    (SAVEPOINT rollback), not via separate connections.
    """
    import psycopg2
    try:
        conn = psycopg2.connect(
            host=os.environ.get('POSTGRES_HOST', 'localhost'),
            port=os.environ.get('POSTGRES_PORT', '5433'),
            database=os.environ.get('POSTGRES_DB', 'olist_dw'),
            user=os.environ.get('POSTGRES_USER', 'admin'),
            password=os.environ.get('POSTGRES_PASSWORD', 'password'),
        )
    except Exception:
        pytest.skip('Postgres not available')
    conn.autocommit = False
    yield conn
    conn.close()


@pytest.fixture
def txn(db_conn):
    """Function-scoped SAVEPOINT wrapper; rolls back after each test.

    HOW IT WORKS:
      Before the test: CREATE SAVEPOINT test_sp
      Test runs:       any INSERTs/UPDATEs/DELETEs happen inside the savepoint
      After the test:  ROLLBACK TO SAVEPOINT test_sp → all changes are undone

    This means tests can write freely to the DB without polluting each other.
    The outer transaction (on db_conn) is never committed, so the real DB stays clean.
    """
    with db_conn.cursor() as cur:
        cur.execute("SAVEPOINT test_sp")
    yield db_conn
    with db_conn.cursor() as cur:
        cur.execute("ROLLBACK TO SAVEPOINT test_sp")


# Minimal bronze data — one row per table, referentially consistent.
# All values are TEXT (matching the bronze schema where everything is TEXT).
# Foreign keys are consistent: order_items references ord_001, prod_001, sell_001, etc.
_BRONZE_SEED = {
    'orders': [
        dict(order_id='ord_001', customer_id='cust_001', order_status='delivered',
             order_purchase_timestamp='2023-01-01 10:00:00',
             order_approved_at='2023-01-01 10:05:00',
             order_delivered_carrier_date='2023-01-02 08:00:00',
             order_delivered_customer_date='2023-01-03 14:00:00',
             order_estimated_delivery_date='2023-01-05 00:00:00'),
    ],
    'customers': [
        dict(customer_id='cust_001', customer_unique_id='cust_u001',
             customer_zip_code_prefix='01310', customer_city='sao paulo', customer_state='SP'),
    ],
    'products': [
        dict(product_id='prod_001', product_category_name='informatica',
             product_name_lenght='50', product_description_lenght='200',
             product_photos_qty='3', product_weight_g='500',
             product_length_cm='30', product_height_cm='10', product_width_cm='20'),
    ],
    'product_category_name_translation': [
        dict(product_category_name='informatica', product_category_name_english='computers'),
    ],
    'sellers': [
        dict(seller_id='sell_001', seller_zip_code_prefix='01310',
             seller_city='sao paulo', seller_state='SP'),
    ],
    'order_items': [
        dict(order_id='ord_001', order_item_id='1', product_id='prod_001',
             seller_id='sell_001', shipping_limit_date='2023-01-02 00:00:00',
             price='99.90', freight_value='15.00'),
    ],
    'order_reviews': [
        dict(review_id='rev_001', order_id='ord_001', review_score='5',
             review_comment_title='Great', review_comment_message='Loved it',
             review_creation_date='2023-01-04 00:00:00',
             review_answer_timestamp='2023-01-04 12:00:00'),
    ],
    'order_payments': [
        dict(order_id='ord_001', payment_sequential='1', payment_type='credit_card',
             payment_installments='3', payment_value='114.90'),
    ],
    'geolocation': [
        dict(geolocation_zip_code_prefix='01310', geolocation_lat='-23.5505',
             geolocation_lng='-46.6333', geolocation_city='sao paulo', geolocation_state='SP'),
    ],
}


@pytest.fixture
def seed_bronze(txn):
    """Inserts a complete minimal bronze dataset. Returns (snapshot_id, run_id).

    Pass *exclude_tables* to omit data rows (manifest entries are still created).

    WHY A UNIQUE SNAPSHOT PER CALL:
      Each call generates a random snapshot_id (e.g. 'test_a1b2c3d4').  This prevents
      collisions with other tests or pre-existing data in the DB.

    WHAT IT INSERTS:
      1. ingestion.runs        — a bronze run marked as 'success'
      2. ingestion.file_manifest — one entry per bronze file (needed by resolve_effective_snapshot)
      3. bronze.*               — one row per table from _BRONZE_SEED (unless excluded)
    """
    from silver.config import TABLE_TO_FILE

    def _seed(exclude_tables=None):
        exclude_tables = exclude_tables or set()
        snapshot_id = f"test_{uuid.uuid4().hex[:8]}"
        run_id = str(uuid.uuid4())

        with txn.cursor() as cur:
            # Register a completed bronze run so the metadata tables are consistent
            cur.execute(
                "INSERT INTO ingestion.runs (run_id, snapshot_id, layer, status) "
                "VALUES (%s, %s, 'bronze', 'success') ON CONFLICT DO NOTHING",
                (run_id, snapshot_id),
            )
            # File manifest entries for ALL tables — resolve_effective_snapshot needs these
            for table, filename in TABLE_TO_FILE.items():
                cur.execute(
                    "INSERT INTO ingestion.file_manifest (snapshot_id, filename, row_count) "
                    "VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                    (snapshot_id, filename, len(_BRONZE_SEED.get(table, []))),
                )
            # Insert actual bronze data rows (skip excluded tables)
            for table, rows in _BRONZE_SEED.items():
                if table in exclude_tables:
                    continue
                for row in rows:
                    cols = list(row.keys()) + ['_snapshot_id', '_run_id', '_source_file']
                    vals = list(row.values()) + [snapshot_id, run_id, TABLE_TO_FILE[table]]
                    ph = ', '.join(['%s'] * len(cols))
                    cur.execute(
                        f"INSERT INTO bronze.{table} ({', '.join(cols)}) VALUES ({ph})", vals,
                    )

        return snapshot_id, run_id

    return _seed


class _NoCommitProxy:
    """Wraps a psycopg2 connection, suppressing commit so the outer txn can rollback everything.

    WHY THIS EXISTS:
      load_silver.load() calls conn.commit() after each table to persist data atomically.
      But we want the txn fixture to roll back ALL changes after the test.
      If we let commit() go through, the data would be permanent and leak between tests.

      This proxy intercepts commit() and turns it into a no-op.  Everything else
      (cursor, execute, fetchone, etc.) passes through to the real connection.

    HOW __getattr__ WORKS:
      Python calls __getattr__ when a normal attribute lookup fails.
      Since we only define commit(), any other attribute (like .cursor()) is forwarded
      to self._conn via __getattr__, which calls getattr(self._conn, name).
    """

    def __init__(self, conn):
        self._conn = conn

    def commit(self):
        pass

    def rollback(self):
        pass    

    def __getattr__(self, name):
        return getattr(self._conn, name)


@pytest.fixture
def load_conn(txn, monkeypatch):
    """Patches get_db_connection and health_check so load() uses the test txn with commit disabled.

    WHAT THIS DOES:
      1. Wraps the txn connection in _NoCommitProxy (commit → no-op).
      2. Replaces get_db_connection with a context manager that yields the proxy.
         This means load()'s `with get_db_connection() as conn:` uses our test connection.
      3. Replaces health_check with a lambda that always returns healthy.
         (The real health_check would try to open a new pooled connection.)

    After the test, monkeypatch automatically restores the original functions,
    and the txn fixture rolls back all database changes.
    """
    proxy = _NoCommitProxy(txn)

    @contextmanager
    def _mock_get_db():
        yield proxy

    monkeypatch.setattr('silver.load_silver.get_db_connection', _mock_get_db)
    monkeypatch.setattr('silver.load_silver.health_check', lambda: {'status': 'healthy'})

    return proxy


@pytest.fixture
def mock_resolve(monkeypatch):
    """Monkeypatches resolve_effective_snapshot to map all bronze tables to the given snapshot.

    WHY: The real resolve_effective_snapshot runs a complex query against ingestion.file_manifest
    to figure out which bronze snapshot to read from.  In tests we control the snapshot_id,
    so we replace the function with a simple lambda that maps every bronze table to that snapshot.
    """

    def _setup(snapshot_id):
        from silver.config import SILVER_TABLE_SOURCES
        all_bronze = set()
        for sources in SILVER_TABLE_SOURCES.values():
            all_bronze.update(sources)
        effective = {t: snapshot_id for t in all_bronze}
        monkeypatch.setattr(
            'silver.load_silver.resolve_effective_snapshot',
            lambda conn, sid: effective,
        )

    return _setup
