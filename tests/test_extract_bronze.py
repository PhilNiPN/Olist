"""
Tests for extract_bronze.py: compute_hash and _count_csv_rows,
the foundations of snapshot IDs and manifest accuracy.
"""

import hashlib
from bronze.extract_bronze import compute_hash, _count_csv_rows


class TestComputeHash:

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