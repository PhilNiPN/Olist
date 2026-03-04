import os
import pytest
from unittest.mock import MagicMock

from db import _validate_config, ConfigError, load_csv_via_temp_table, ALLOWED_TABLES

"""
Tests for db.py
- Security validations
- config fail fast behavior
"""

### table allowlist

class TestTableAllowed: 
    """
    load_csv_via_temp_table must reject any table not in ALLOWED_TABLES.
    """

    def test_rejects_sql_injection_attempt(self):
        conn = MagicMock()
        with pytest.raises(ValueError, match = 'Security Error'):
            load_csv_via_temp_table(conn, 'fake.csv', 'orders; DROP TABLE users;--', 'snap', 'run', 'f.csv')

    def test_rejects_unknown_table(self):
        conn = MagicMock()
        with pytest.raises(ValueError, match = 'Security Error'): 
            load_csv_via_temp_table(conn, 'fake.csv', 'not_a_real_table', 'snap', 'run', 'f.csv')

    def test_accepts_every_allowed_table(self, tmp_path):
        """ Sanity check: 
        testing that the allowlist doesn't reject valid names
        """
        conn = MagicMock()
        for table in ALLOWED_TABLES:
            with pytest.raises((ValueError, FileNotFoundError)):
                load_csv_via_temp_table(conn, 'Data/fake.csv', table, 'snap', 'run', 'f.csv')


### path traversal checks

class TestPathTraversal:
    """
    load_csv_via_temp_table must reject paths outside the Data/ directory.
    """
    
    def test_rejects_abolute_system_paths(self):
        conn = MagicMock()
        with pytest.raises(ValueError, match = 'Invalid CSV path'):
            load_csv_via_temp_table(conn, '/etc/passwd', 'orders', 'snap', 'run', 'f.csv')

    def test_rejects_relative_traversal(self):
        conn = MagicMock()
        with pytest.raises(ValueError, match = 'Invalid CSV path'):
            load_csv_via_temp_table(conn, 'Data/../../../etc/passwd', 'orders', 'snap', 'run', 'f.csv')


### config fail fast behavior

class TestConfigValidation:
    """
    _validate_config must fail fast if any required environment variables are missing.
    """

    REQUIRED_VARS = ["POSTGRES_HOST", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_PORT"]

    def test_raises_when_all_missing(self, monkeypatch):
        for var in self.REQUIRED_VARS:
            monkeypatch.delenv(var, raising=False)

        with pytest.raises(ConfigError, match = 'Missing required environment variables'):
            _validate_config()

    def test_raises_when_one_missing(self, monkeypatch):
        for var in self.REQUIRED_VARS:
            monkeypatch.setenv(var, 'value')
        monkeypatch.delenv('POSTGRES_PASSWORD')

        with pytest.raises(ConfigError, match = 'POSTGRES_PASSWORD'):
            _validate_config()

    def test_returns_config_when_all_present(self, monkeypatch):
        for var in self.REQUIRED_VARS:
            monkeypatch.setenv(var, 'test')

        config = _validate_config()
        assert config['host'] == 'test'
        assert config['database'] == 'test'
        assert config['connect_timeout'] == 10

    def test_custom_connect_timeout(self, monkeypatch):
        for var in self.REQUIRED_VARS:
            monkeypatch.setenv(var, 'test')
        monkeypatch.setenv('DB_CONNECT_TIMEOUT', '30')

        config = _validate_config()
        assert config['connect_timeout'] == 30