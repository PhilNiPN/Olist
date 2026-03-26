"""
Tests for extract_bronze.py: compute_hash and _count_csv_rows,
the foundations of snapshot IDs and manifest accuracy.

WHAT THESE TESTS COVER:
  compute_hash  — SHA-256 hashing of raw source files.  The hash is used as
                  the snapshot ID, so correctness here guarantees idempotency:
                  same file → same snapshot → no duplicate loads.
  _count_csv_rows — Counts data rows (excluding header).  The count goes into
                    the file manifest and later feeds the row_count quality check.

WHY UNIT TESTS ARE ENOUGH:
  Both functions are pure (file in → value out) with no DB interaction.
  We use pytest's tmp_path fixture to create throwaway files on disk,
  so no mocking or fixtures from conftest.py are needed.
"""

import hashlib
from bronze.extract_bronze import compute_hash, _count_csv_rows


class TestComputeHash:
    """
    WHAT: Verify compute_hash returns a correct, deterministic SHA-256 hex digest.

    WHY:  The hash IS the snapshot ID.  If it's wrong or non-deterministic,
          the idempotency gate (_file_changed) breaks and files get re-loaded
          or skipped incorrectly.

    TECHNIQUE: Write known bytes to tmp_path files and compare against hashlib directly.
    """
    def test_sha256_matches_known_value(self, tmp_path):
        f = tmp_path / "test.bin"
        f.write_bytes(b"hello world 123123")
        expected = hashlib.sha256(b"hello world 123123").hexdigest()
        assert compute_hash(f) == expected

    def test_different_content_produces_different_hash(self, tmp_path):
        a = tmp_path / "a.bin"
        b = tmp_path / "b.bin"
        a.write_bytes(b"file one")
        b.write_bytes(b"file two")
        assert compute_hash(a) != compute_hash(b)

    def test_same_content_produces_same_hash(self, tmp_path):
        a = tmp_path / "a.bin"
        b = tmp_path / "b.bin"
        a.write_bytes(b"identical")
        b.write_bytes(b"identical")
        assert compute_hash(a) == compute_hash(b)


class TestCountCsvRows:
    """
    WHAT: Verify _count_csv_rows counts data rows and excludes the header.
    
    WHY:  The count is stored in the file manifest and later compared by
          check_row_count (quality check).  Off-by-one here means false DQ failures.
    
    TECHNIQUE: Write CSV strings to tmp_path files with known row counts.
    """
    def test_counts_data_rows_only(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("col1,col2\na,b\nc,d\ne,f\n", encoding="utf-8")
        assert _count_csv_rows(f) == 3

    def test_header_only_returns_zero(self, tmp_path):
        f = tmp_path / "empty.csv"
        f.write_text("col1,col2\n", encoding="utf-8")
        assert _count_csv_rows(f) == 0

    def test_handles_large_row_count(self, tmp_path):
        f = tmp_path / "big.csv"
        lines = ["id,value"] + [f"{i},data" for i in range(10_000)]
        f.write_text("\n".join(lines), encoding="utf-8")
        assert _count_csv_rows(f) == 10_000