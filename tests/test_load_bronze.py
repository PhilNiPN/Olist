"""
Tests for load_bronze.py: the _file_changed function that controls
whether a file gets re-loaded (idempotency gate).
"""
from bronze.load_bronze import _file_changed


class TestFileChanged:

    def test_true_when_never_loaded_before(self, mock_conn):
        """
        First load: no row in file_manifest, so file is 'changed'.
        """
        conn, _ = mock_conn(fetchone=None)
        assert _file_changed(conn, "orders.csv", "abc123") is True

    def test_true_when_hash_differs(self, mock_conn):
        """
        Source file changed since last load.
        """
        conn, _ = mock_conn(fetchone=("old_hash_value",))
        assert _file_changed(conn, "orders.csv", "new_hash_value") is True

    def test_false_when_hash_matches(self, mock_conn):
        """
        File unchanged — skip loading.
        """
        conn, _ = mock_conn(fetchone=("same_hash",))
        assert _file_changed(conn, "orders.csv", "same_hash") is False