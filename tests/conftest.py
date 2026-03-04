import pytest
from unittest.mock import MagicMock

@pytest.fixture
def mock_conn():
    """
    Returns a factory that creates mock psycopg2 connections.
    Supports conn.cursor() as a context manager, matching how all our modules use it.

    usage: 
     conn, cur = mock_conn(fetchone=(100,))
     conn, cur = mock_conn(fetchall=[('col_a',), ('col_b',)]) 
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