"""Tests for serializer module."""

from __future__ import annotations

import pickle
import tempfile
from pathlib import Path

import pandas as pd
import pyarrow as pa

from strata.notebook.serializer import (
    deserialize_value,
    serialize_value,
)


# Module-level classes for pickle tests (local classes can't be pickled)
class _PickleTestCustomClass:
    def __init__(self, x):
        self.x = x


class _PickleTestMyModel:
    def __init__(self, name, value):
        self.name = name
        self.value = value

    def __eq__(self, other):
        return self.name == other.name and self.value == other.value


class _SerializerNoStatePerson:
    name = "John"
    age = 20

    def __str__(self):
        return f"{self.name}:{self.age}"


class TestArrowSerialization:
    """Test Arrow IPC serialization."""

    def test_serialize_dataframe(self):
        """Test serializing a pandas DataFrame."""
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})

        with tempfile.TemporaryDirectory() as tmpdir:
            result = serialize_value(df, Path(tmpdir), "df")

            assert result["content_type"] == "arrow/ipc"
            assert result["rows"] == 3
            assert result["columns"] == ["a", "b"]
            assert result["bytes"] > 0
            assert result["preview"] == [[1, 4.0], [2, 5.0], [3, 6.0]]

    def test_serialize_arrow_table(self):
        """Test serializing a PyArrow Table."""
        table = pa.table({"x": [10, 20, 30], "y": ["a", "b", "c"]})

        with tempfile.TemporaryDirectory() as tmpdir:
            result = serialize_value(table, Path(tmpdir), "tbl")

            assert result["content_type"] == "arrow/ipc"
            assert result["rows"] == 3
            assert result["columns"] == ["x", "y"]

    def test_roundtrip_dataframe(self):
        """Test round-trip: serialize and deserialize a DataFrame."""
        df_orig = pd.DataFrame(
            {"id": [1, 2, 3], "value": [1.5, 2.5, 3.5], "name": ["a", "b", "c"]}
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            # Serialize
            meta = serialize_value(df_orig, tmpdir, "data")
            file_path = tmpdir / meta["file"]

            # Deserialize
            df_loaded = deserialize_value(meta["content_type"], file_path)

            # Convert to pandas for comparison
            if isinstance(df_loaded, pa.Table):
                df_loaded = df_loaded.to_pandas()

            # Check shape and values
            assert df_loaded.shape == df_orig.shape
            assert list(df_loaded.columns) == list(df_orig.columns)
            pd.testing.assert_frame_equal(df_loaded, df_orig)

    def test_serialize_arrow_with_nulls(self):
        """Test Arrow serialization with null values."""
        df = pd.DataFrame({"a": [1, None, 3], "b": [None, 2.0, 3.0]})

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            meta = serialize_value(df, tmpdir, "nulls")
            file_path = tmpdir / meta["file"]

            result = deserialize_value(meta["content_type"], file_path)
            if isinstance(result, pa.Table):
                result = result.to_pandas()

            # Verify nulls are preserved
            assert result.iloc[0, 0] == 1
            assert pd.isna(result.iloc[1, 0])
            assert pd.isna(result.iloc[0, 1])


class TestJsonSerialization:
    """Test JSON serialization."""

    def test_serialize_dict(self):
        """Test serializing a dictionary."""
        data = {"x": 1, "y": "hello", "z": [1, 2, 3]}

        with tempfile.TemporaryDirectory() as tmpdir:
            result = serialize_value(data, Path(tmpdir), "data")

            assert result["content_type"] == "json/object"
            assert result["bytes"] > 0
            assert result["preview"] == data

    def test_serialize_list(self):
        """Test serializing a list."""
        data = [1, 2, 3, "hello"]

        with tempfile.TemporaryDirectory() as tmpdir:
            result = serialize_value(data, Path(tmpdir), "lst")

            assert result["content_type"] == "json/object"
            assert result["preview"] == data

    def test_serialize_scalar(self):
        """Test serializing scalar values."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Integer
            result = serialize_value(42, Path(tmpdir), "int_val")
            assert result["content_type"] == "json/object"

            # String
            result = serialize_value("hello", Path(tmpdir), "str_val")
            assert result["content_type"] == "json/object"

            # Boolean
            result = serialize_value(True, Path(tmpdir), "bool_val")
            assert result["content_type"] == "json/object"

    def test_roundtrip_dict(self):
        """Test round-trip for dictionary."""
        data_orig = {"a": 1, "b": "test", "c": [1, 2, 3], "d": None}

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            meta = serialize_value(data_orig, tmpdir, "data")
            file_path = tmpdir / meta["file"]
            data_loaded = deserialize_value(meta["content_type"], file_path)

            assert data_loaded == data_orig

    def test_roundtrip_nested(self):
        """Test round-trip for nested structure."""
        data_orig = {
            "users": [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
            ],
            "count": 2,
            "active": True,
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            meta = serialize_value(data_orig, tmpdir, "nested")
            file_path = tmpdir / meta["file"]
            data_loaded = deserialize_value(meta["content_type"], file_path)

            assert data_loaded == data_orig


class TestPickleSerialization:
    """Test pickle serialization."""

    def test_serialize_custom_object(self):
        """Test serializing a custom object."""
        obj = _PickleTestCustomClass(42)

        with tempfile.TemporaryDirectory() as tmpdir:
            result = serialize_value(obj, Path(tmpdir), "obj")

            assert result["content_type"] == "pickle/object"
            assert result["codec"] == "pickle"
            assert result["type"] == "_PickleTestCustomClass"
            assert result["bytes"] > 0

    def test_roundtrip_custom_object(self):
        """Test round-trip for custom object."""
        obj_orig = _PickleTestMyModel("test", 123)

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            meta = serialize_value(obj_orig, tmpdir, "model")
            file_path = tmpdir / meta["file"]
            obj_loaded = deserialize_value(meta["content_type"], file_path)

            assert obj_loaded == obj_orig

    def test_pickle_serialization_uses_codec_envelope(self):
        """Pickle/object files should store a codec envelope for future backends."""
        obj = _PickleTestCustomClass(42)

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            meta = serialize_value(obj, tmpdir, "obj")
            file_path = tmpdir / meta["file"]

            with open(file_path, "rb") as f:
                payload = pickle.load(f)

            assert payload["__strata_object_codec__"] == "strata.notebook.object_codec.v1"
            assert payload["codec"] == "pickle"
            assert isinstance(payload["payload"], bytes)

    def test_deserialize_legacy_raw_pickle(self):
        """Legacy raw-pickle files should remain readable after codec abstraction."""
        obj = _PickleTestMyModel("legacy", 7)

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            file_path = tmpdir / "legacy.pickle"
            with open(file_path, "wb") as f:
                pickle.dump(obj, f, protocol=5)

            loaded = deserialize_value("pickle/object", file_path)
            assert loaded == obj

    def test_roundtrip_cell_instance_without_instance_state(self):
        """module/cell-instance should restore plain class-var instances with no __dict__ state."""
        import sys

        person = _SerializerNoStatePerson()
        module = sys.modules[_SerializerNoStatePerson.__module__]
        module.__dict__["__strata_cell_module__"] = True
        module.__dict__[
            "__strata_cell_module_source__"
        ] = (
            "class _SerializerNoStatePerson:\n"
            "    name = 'John'\n"
            "    age = 20\n"
            "\n"
            "    def __str__(self):\n"
            "        return f\"{self.name}:{self.age}\"\n"
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            meta = serialize_value(person, tmpdir, "p")
            assert meta["content_type"] == "module/cell-instance"

            loaded = deserialize_value(meta["content_type"], tmpdir / meta["file"])
            assert str(loaded) == "John:20"

    def test_serialize_unpicklable_returns_error(self):
        """Test that unpicklable objects return an error result."""
        # Lambdas defined locally can't be pickled
        def func(x):
            return x + 1

        with tempfile.TemporaryDirectory() as tmpdir:
            result = serialize_value(func, Path(tmpdir), "func")

            # Should return error metadata instead of crashing
            assert result.get("error") is not None or result["content_type"] == "pickle/object"


class TestContentTypeDetection:
    """Test content type detection."""

    def test_detect_dataframe(self):
        """Detect DataFrame as arrow/ipc."""
        from strata.notebook.serializer import detect_content_type

        df = pd.DataFrame({"a": [1, 2, 3]})
        assert detect_content_type(df) == "arrow/ipc"

    def test_detect_arrow_table(self):
        """Detect Arrow Table as arrow/ipc."""
        from strata.notebook.serializer import detect_content_type

        table = pa.table({"a": [1, 2, 3]})
        assert detect_content_type(table) == "arrow/ipc"

    def test_detect_dict(self):
        """Detect dict as json/object."""
        from strata.notebook.serializer import detect_content_type

        assert detect_content_type({"a": 1}) == "json/object"

    def test_detect_list(self):
        """Detect list as json/object."""
        from strata.notebook.serializer import detect_content_type

        assert detect_content_type([1, 2, 3]) == "json/object"

    def test_detect_scalar(self):
        """Detect scalars as json/object."""
        from strata.notebook.serializer import detect_content_type

        assert detect_content_type(42) == "json/object"
        assert detect_content_type("hello") == "json/object"
        assert detect_content_type(True) == "json/object"

    def test_detect_custom_object(self):
        """Detect custom object as pickle/object."""
        from strata.notebook.serializer import detect_content_type

        class MyClass:
            pass

        assert detect_content_type(MyClass()) == "pickle/object"


class TestLargeDataFrames:
    """Test serialization of larger DataFrames."""

    def test_serialize_large_dataframe(self):
        """Test serializing a larger DataFrame (1000 rows)."""
        df = pd.DataFrame(
            {
                "id": range(1000),
                "value": [float(i) * 1.5 for i in range(1000)],
                "category": ["A", "B", "C"] * 333 + ["A"],
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            meta = serialize_value(df, tmpdir, "large")

            assert meta["content_type"] == "arrow/ipc"
            assert meta["rows"] == 1000
            # Preview should only have first 20 rows
            assert len(meta["preview"]) == 20

            # Verify round-trip
            file_path = tmpdir / meta["file"]
            df_loaded = deserialize_value(meta["content_type"], file_path)
            if isinstance(df_loaded, pa.Table):
                df_loaded = df_loaded.to_pandas()

            pd.testing.assert_frame_equal(df_loaded, df)
