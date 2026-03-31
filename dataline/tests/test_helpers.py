"""Tests for sandbox helper functions (Phase 6)."""

import os
import tempfile

import numpy as np
import pandas as pd
import pytest

from dataline.helpers.data_helpers import (
    clean_numeric,
    describe_df,
    detect_date_columns,
    find_join_keys,
    load_intermediate,
    safe_read_csv,
    safe_read_json,
    save_intermediate,
)


@pytest.fixture
def tmp_task_dir():
    """Create a temporary task directory with sample files."""
    with tempfile.TemporaryDirectory() as td:
        # CSV file
        df = pd.DataFrame({"id": [1, 2, 3], "amount": [10.5, 20.3, 30.1], "name": ["a", "b", "c"]})
        df.to_csv(os.path.join(td, "test.csv"), index=False)

        # JSON file
        import json
        with open(os.path.join(td, "test.json"), "w") as f:
            json.dump({"key": "value", "items": [1, 2, 3]}, f)

        yield td


@pytest.fixture
def tmp_temp_dir():
    """Create a temporary TEMP_DIR for pickle tests."""
    with tempfile.TemporaryDirectory() as td:
        yield td


# --- safe_read_csv ---


class TestSafeReadCsv:
    def test_reads_csv(self, tmp_task_dir: str):
        df = safe_read_csv("test.csv", task_dir=tmp_task_dir)
        assert len(df) == 3
        assert "id" in df.columns
        assert "amount" in df.columns

    def test_absolute_path(self, tmp_task_dir: str):
        path = os.path.join(tmp_task_dir, "test.csv")
        df = safe_read_csv(path)
        assert len(df) == 3

    def test_kwargs_passed(self, tmp_task_dir: str):
        df = safe_read_csv("test.csv", task_dir=tmp_task_dir, nrows=2)
        assert len(df) == 2


# --- safe_read_json ---


class TestSafeReadJson:
    def test_reads_json(self, tmp_task_dir: str):
        data = safe_read_json("test.json", task_dir=tmp_task_dir)
        assert data["key"] == "value"
        assert data["items"] == [1, 2, 3]


# --- describe_df ---


class TestDescribeDf:
    def test_output_contains_shape(self):
        df = pd.DataFrame({"x": [1, 2], "y": ["a", "b"]})
        result = describe_df(df, "test")
        assert "2 rows" in result
        assert "2 cols" in result

    def test_output_contains_columns(self):
        df = pd.DataFrame({"price": [1.0, None, 3.0], "name": ["a", "b", "c"]})
        result = describe_df(df, "test")
        assert "price" in result
        assert "1 nulls" in result
        assert "name" in result


# --- find_join_keys ---


class TestFindJoinKeys:
    def test_finds_shared_columns(self):
        df_a = pd.DataFrame({"id": [1], "name": ["a"], "x": [10]})
        df_b = pd.DataFrame({"id": [1], "name": ["b"], "y": [20]})
        keys = find_join_keys(df_a, df_b)
        assert sorted(keys) == ["id", "name"]

    def test_no_shared_columns(self):
        df_a = pd.DataFrame({"x": [1]})
        df_b = pd.DataFrame({"y": [2]})
        assert find_join_keys(df_a, df_b) == []

    def test_case_insensitive(self):
        df_a = pd.DataFrame({"ID": [1]})
        df_b = pd.DataFrame({"id": [2]})
        keys = find_join_keys(df_a, df_b)
        assert len(keys) == 1


# --- detect_date_columns ---


class TestDetectDateColumns:
    def test_detects_iso_dates(self):
        df = pd.DataFrame({
            "created": ["2024-01-15", "2024-02-20", "2024-03-10"],
            "amount": [10, 20, 30],
        })
        dates = detect_date_columns(df)
        assert "created" in dates
        assert "amount" not in dates

    def test_detects_datetime_dtype(self):
        df = pd.DataFrame({
            "ts": pd.to_datetime(["2024-01-01", "2024-02-01", "2024-03-01"]),
        })
        dates = detect_date_columns(df)
        assert "ts" in dates

    def test_no_false_positive_on_strings(self):
        df = pd.DataFrame({"name": ["alice", "bob", "charlie"]})
        assert detect_date_columns(df) == []


# --- clean_numeric ---


class TestCleanNumeric:
    def test_currency_symbols(self):
        s = pd.Series(["$1,234.56", "€100", "£50.00"])
        result = clean_numeric(s)
        assert result.iloc[0] == pytest.approx(1234.56)
        assert result.iloc[1] == pytest.approx(100.0)
        assert result.iloc[2] == pytest.approx(50.0)

    def test_percentages(self):
        s = pd.Series(["45.6%", "100%", "0.5%"])
        result = clean_numeric(s)
        assert result.iloc[0] == pytest.approx(45.6)

    def test_already_numeric(self):
        s = pd.Series([1.0, 2.0, 3.0])
        result = clean_numeric(s)
        assert result.iloc[0] == 1.0

    def test_non_convertible_becomes_nan(self):
        s = pd.Series(["hello", "world"])
        result = clean_numeric(s)
        assert result.isna().all()


# --- save/load intermediate ---


class TestIntermediateStorage:
    def test_roundtrip_dataframe(self, tmp_temp_dir: str):
        df = pd.DataFrame({"a": [1, 2, 3]})
        save_intermediate(df, "test_df", temp_dir=tmp_temp_dir)
        loaded = load_intermediate("test_df", temp_dir=tmp_temp_dir)
        pd.testing.assert_frame_equal(df, loaded)

    def test_roundtrip_dict(self, tmp_temp_dir: str):
        data = {"key": "value", "num": 42}
        save_intermediate(data, "test_dict", temp_dir=tmp_temp_dir)
        loaded = load_intermediate("test_dict", temp_dir=tmp_temp_dir)
        assert loaded == data

    def test_auto_adds_pkl_extension(self, tmp_temp_dir: str):
        save_intermediate([1, 2, 3], "my_list", temp_dir=tmp_temp_dir)
        assert os.path.exists(os.path.join(tmp_temp_dir, "my_list.pkl"))

    def test_explicit_pkl_extension(self, tmp_temp_dir: str):
        save_intermediate([1], "data.pkl", temp_dir=tmp_temp_dir)
        loaded = load_intermediate("data.pkl", temp_dir=tmp_temp_dir)
        assert loaded == [1]
