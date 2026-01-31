"""
Database connection and loading utility for Olist data warehouse.

this module provides:
- connection pooling with automatic retries on transient errors.
- secure csv loading via temp table pattern with metadata tracking.
- idempotent loading with snapshot-based deduplication.
- health monitoring for database connections.

Usage:
 from db import get_db_connection, load_csv_via_temp_table

 with get_db_connection() as conn:
     load_csv_via_temp_table(conn, "Data/orders.csv", "orders", "2024-01-01", "run-123")
Exports:
 - get_db_connection: Context manager for pooled connections
 - load_csv_via_temp_table: Idempotent CSV loader for bronze layer
 - health_check: Database connectivity check
 - close_pool: Graceful shutdown
"""

import os 
import logging
from contextlib import contextmanager
from typing import Generator, Optional
from dataclasses import dataclass
import time
import psycopg2
from psycopg2 import pool, sql, extensions, OperationalError, InterfaceError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# configure logger
logger = logging.getLogger(__name__)

__all__ = [
    # Exceptions
    "DatabaseError",
    "DbConnectionError",
    "LoadError",
    "ConfigError",
    # result types
    "LoadResult",
    # Connection
    "get_db_connection",
    "close_pool",
    # Health
    "health_check",
    # Loading
    "load_csv_via_temp_table",
    # Constants
    "ALLOWED_TABLES",
]

###############################################
############   custom exceptions   ############
###############################################

class DatabaseError(Exception):
    """ base exception for database operations. """
    pass

class DbConnectionError(DatabaseError):
    """ failed to connect to database. """
    pass

class LoadError(DatabaseError):
    """ failed to load into table. """
    def __init__(self, table: str, message: str):
        self.table = table
        super().__init__(f"Failed to load into table {table}: {message}")

class ConfigError(DatabaseError):
    """ failed validation of configuration or environment variables. """
    pass


###########################################
############   result types   #############
###########################################

# result type for load operations
@dataclass
class LoadResult:
    """ result of a CSV load operation. """
    table: str
    snapshot_id: str
    run_id: str
    rows_inserted: int
    rows_deleted: int
    duration_seconds: float
    
    @property
    def net_rows(self) -> int:
        """Net change in rows."""
        return self.rows_inserted - self.rows_deleted


#######################################
############   constants   ############
#######################################

# define allowed tables for security
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

METADATA_COLS = ['_snapshot_id', '_run_id', '_inserted_at']


##########################################
############   module state   ############
##########################################

# define global connection pool
_DB_POOL: Optional[pool.ThreadedConnectionPool] = None


####################################
############   config   ############
####################################

# validate environment variables to fail fast
def _validate_config() -> dict:
    """ validate required environment variables."""
    required_vars = ["POSTGRES_HOST", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_PORT"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]

    if missing_vars:
        raise ConfigError(f"Missing required environment variables: {missing_vars}")

    return {
        "host": os.getenv("POSTGRES_HOST"),
        "database": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "port": os.getenv("POSTGRES_PORT"),
        "connect_timeout": int(os.getenv("DB_CONNECT_TIMEOUT", 10)),
    }


#############################################
############   pool management   ############
#############################################

# initialize the global connection pool
def _init_db_pool() -> None:
    """ initialize the global connection pool."""
    global _DB_POOL

    # config loading
    config = _validate_config()

    min_conn = int(os.getenv("DB_POOL_MIN", 2))
    max_conn = int(os.getenv("DB_POOL_MAX", 20))

    # create pool
    try:
        _DB_POOL = psycopg2.pool.ThreadedConnectionPool(
            minconn=min_conn,
            maxconn=max_conn,
            **config
        )
        logger.info("pool_created", extra={
            "min_conn": min_conn,
            "max_conn": max_conn
        })
    except psycopg2.Error as e:
        logger.error("pool_creation_failed", extra={"error": str(e)})
        raise

def close_pool() -> None:
    """ close all connections in the global connection pool. """
    global _DB_POOL
    if _DB_POOL:
        _DB_POOL.closeall()
        _DB_POOL = None
        logger.info("pool_closed")
    else:
        logger.info("pool_already_closed")


###################################################
############   connection management   ############
###################################################

# retry on transient errors
@retry(retry=retry_if_exception_type((OperationalError, InterfaceError)), 
       stop=stop_after_attempt(3), 
       wait=wait_exponential(multiplier=0.5, min=1, max=5))
def _acquire_connection() -> extensions.connection:
    """ helper to retry pool access """
    if _DB_POOL is None:
        _init_db_pool()
    return _DB_POOL.getconn()

@contextmanager
def get_db_connection() -> Generator[extensions.connection, None, None]: 
    """ 
    context manager for database connection.
    handles pooling, error logging, and automatic rollbacks.
    """
    if _DB_POOL is None:
        _init_db_pool()

    # use retry logic to acquire connection
    conn = _acquire_connection()

    try:
        yield conn
    # rollback on error - reset connection state before returning to pool
    except Exception as e:
        logger.error("transaction_error", extra={"error": str(e)})
        if conn:
            conn.rollback()
        raise DbConnectionError(str(e)) from e
    finally:
        if conn:
            # close connection if it is broken
            if conn.closed or not _is_alive(conn):
                _DB_POOL.putconn(conn, close=True)
            else:
                # return connection to pool
                _DB_POOL.putconn(conn)
                logger.debug("connection_returned")


####################################
############   health   ############
####################################

def health_check() -> dict:
    """ check database connection health for monitoring purposes. """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")

        pool_stats = {}
        if _DB_POOL:
            pool_stats = {
                "min_conn": _DB_POOL.minconn,
                "max_conn": _DB_POOL.maxconn,
                "available": len(_DB_POOL._pool),
                "in_use": len(_DB_POOL._used),
                "closed": _DB_POOL.closed,
            }

        return {
            "status": "healthy", 
            "database": os.getenv("POSTGRES_DB"),
            "pool": pool_stats}

    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}

def _is_alive(conn: extensions.connection) -> bool:
    """ pings connection to check if it is still alive. """
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        return True
    except (OperationalError, InterfaceError):
        return False

##########################################
############   data loading   ############
##########################################

def load_csv_via_temp_table(conn: extensions.connection, csv_path: str, table_name: str, snapshot_id: str, run_id: str) -> LoadResult:
    """ 
    load a CSV file into a bronze table using a temp table pattern. 
    1. create temp table (matching/inherits target structure).
    2. drop metadata columns from temp table so it matches CSV exactly.
    3. copy data from CSV.
    4. insert into target table with metadata.
    """
    # validate table name against allowlist for security
    if table_name not in ALLOWED_TABLES:
        raise ValueError(f"Security Error: Invalid table name '{table_name}'")

    # validate csv path to prevent traversal
    base_dir = os.path.abspath(os.path.join(os.getcwd(), 'Data'))
    abs_path = os.path.abspath(csv_path)

    if os.path.commonpath([base_dir, abs_path]) != base_dir:
        logger.warning("path_traversal_attempt", extra={"path": abs_path})
        raise ValueError("Invalid CSV path")

    # check if file exists
    if not os.path.exists(abs_path):
        raise FileNotFoundError(f"CSV file not found: {abs_path}")

    start_time = time.time()

    # unique temp table names to avoid collisions
    tmp_table_name = f"tmp_{table_name}_{run_id.replace('-', '')}"

    # database operations
    try:
        with conn.cursor() as cur:
            # defining identifiers safely
            tmp_table = sql.Identifier(tmp_table_name)
            target_table = sql.Identifier("bronze", table_name)

            # create temp table
            query = sql.SQL("CREATE TEMP TABLE {} (LIKE {} INCLUDING ALL) ON COMMIT DROP;").format(tmp_table, target_table)
            cur.execute(query)

            # drop metadata columns
            for col in METADATA_COLS:
                query = sql.SQL("ALTER TABLE {} DROP COLUMN IF EXISTS {};").format(tmp_table, sql.Identifier(col))
                cur.execute(query)

            # copy data
            query = sql.SQL("COPY {} FROM STDIN WITH CSV HEADER").format(tmp_table)
            with open(abs_path, 'r', encoding='utf-8') as f:
                cur.copy_expert(query, f)

            # audit - check if data already exists for this snapshot_id
            query = sql.SQL("SELECT COUNT(*) FROM {} WHERE _snapshot_id = %s;").format(target_table)
            cur.execute(query, (snapshot_id,)) 
            existing_rows = cur.fetchone()[0]
            if existing_rows > 0:
                logger.warning("replacing_existing_data", extra={
                    "table": table_name,
                    "snapshot_id": snapshot_id,
                    "rows_replaced": existing_rows,
                })

            # idempocency - clear existing data for this snapshot_id
            query = sql.SQL("DELETE FROM {} WHERE _snapshot_id = %s;").format(target_table)
            cur.execute(query, (snapshot_id,))
            deleted_rows = cur.rowcount

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
            duration = time.time() - start_time
            logger.info("csv_load_success", extra={
                "table": table_name,
                "rows": row_count,
                "snapshot_id": snapshot_id,
                "run_id": run_id,
                "duration_seconds": round(duration, 3)
            })
            return LoadResult(
                table=table_name,
                snapshot_id=snapshot_id,
                run_id=run_id,
                rows_inserted=row_count,
                rows_deleted=deleted_rows,
                duration_seconds=round(duration, 3)
            )

    except Exception as e: 
        logger.error("csv_load_failed", extra={
            "table": table_name,
            "error": str(e)
        })
        conn.rollback()
        raise LoadError(table_name, str(e)) from e