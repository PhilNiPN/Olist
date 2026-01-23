import os 
import logging
from contextlib import contextmanager
from typing import Generator, Optional, Any

import psycopg2
from psycopg2 import pool, sql, extensions 

"""
Database connection utility

this is to keep the connection logic central and handles the temp table loading pattern cleanly.
"""
# configure logger
logger = logging.getLogger(__name__)

# Constants - # Define allowed tables for security
ALLOWED_TABLES = {
    'orders', 
    'order_items', 
    'customers', 
    'products', 
    'sellers',
    'order_reviews', 
    'order_payments', 
    'geolocation',
    'product_category_name_translation'
}

# Define global connection pool
_DB_POOL: Optional[pool.ThreadedConnectionPool] = None

def _init_db_pool() -> None:
    """ Initialize the global connection pool."""
    global _DB_POOL

    # configuration and validation
    db_password = os.getenv("POSTGRES_PASSWORD")
    if not db_password:
        raise ValueError("Environment variable POSTGRES_PASSWORD is missing!")

    db_config = {
        "host":     os.getenv("POSTGRES_HOST"),
        "database": os.getenv("POSTGRES_DB"),
        "user":     os.getenv("POSTGRES_USER"),
        "password": db_password,
        "port":     os.getenv("POSTGRES_PORT")
    }

    # create pool
    try:
        _DB_POOL = psycopg2.pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=10,
            **db_config
        )
        logger.info("Database connection pool created successfully.")
    except psycopg2.Error as e:
        logger.error(f"Failed to initialize database connection pool: {e}")
        raise

@contextmanager
def get_db_connection() -> Generator[extensions.connection, None, None]: 
    """ 
    Context manager for database connection.
    Handles pooling, error logging, and automatic rollbacks.
    """
    if _DB_POOL is None:
        _init_db_pool()

    # get connection from pool
    conn = _DB_POOL.getconn()
    try:
        yield conn
    # rollback on error - reset connection state before returning to pool
    except Exception as e:
        logger.error(f"Database transaction failed: {e}")
        conn.rollback()
        raise
    finally:
        # return connection to pool
        _DB_POOL.putconn(conn)
        logger.info("Connection returned to pool.")

def execute_query(conn: extensions.connection, query: str, params: Optional[tuple] = None) -> None: 
    """ Execute a query on the database. """
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            conn.commit()
    except Exception as e:
        logger.error(f"Database query failed: {query[:50]}... | Error: {e}")
        conn.rollback()
        raise

def load_csv_via_temp_table(conn, csv_path, table_name, snapshot_id, run_id):
    """ 
    Load a CSV file into a bronze table using a temp table pattern. 
    1. Create temp table (matching/inherits target structure).
    2. Drop metadata columns from temp table so it matches CSV exactly.
    3. Copy data from CSV.
    4. Insert into target table with metadata.
    """
    # Validate table name against allowlist for security
    if table_name not in ALLOWED_TABLES:
        raise ValueError(f"Security Error: Invalid table name '{table_name}'")

    # Validate csv path to prevent traversal
    # Assumes all CSVs should be within the project root or a specific data dir
    base_dir = os.path.abspath(os.path.join(os.getcwd(), 'Data'))
    abs_path = os.path.abspath(csv_path)
    if not abs_path.startswith(base_dir):
        logger.warning(f"Security Alert: Blocked path traversal attempt: {abs_path}")
        raise ValueError("Invalid CSV path")

    # database operations
    try:
        with conn.cursor() as cur:
            # defining identifiers safely
            tmp_table = sql.Identifier(f"tmp_{table_name}")
            target_table = sql.Identifier("bronze", table_name)

            # create temp table
            query = sql.SQL("CREATE TEMP TABLE {} (LIKE {} INCLUDING ALL) ON COMMIT DROP;").format(tmp_table, target_table)
            cur.execute(query)

            # drop metadata columns
            for col in ['_snapshot_id', '_run_id', '_inserted_at']:
                query = sql.SQL("ALTER TABLE {} DROP COLUMN {};").format(tmp_table, sql.Identifier(col))
                cur.execute(query)

            # copy data
            query = sql.SQL("COPY {} FROM STDIN WITH CSV HEADER").format(tmp_table)
            with open(abs_path, 'r', encoding='utf-8') as f:
                cur.copy_expert(query, f)

            # insert into target table
            query = sql.SQL("""
            INSERT INTO {} 
            SELECT *, %s, %s, NOW()
            FROM {};
            """).format(target_table, tmp_table)
            cur.execute(query, (snapshot_id, run_id))

            # get row count
            query = sql.SQL("SELECT COUNT(*) FROM {};").format(tmp_table)
            cur.execute(query)
            row_count = cur.fetchone()[0]

            conn.commit()
            logger.info(f"Successfully loaded {row_count} rows into {table_name}")
            return row_count

    except Exception as e: 
        logger.error(f"Failed to load batch for {table_name}: {e}")
        conn.rollback()
        raise