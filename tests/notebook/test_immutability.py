"""Tests for immutability detection (M6)."""

from __future__ import annotations

import pandas as pd

from strata.notebook.immutability import (
    InputSnapshot,
    apply_defensive_copy,
    detect_mutations,
    snapshot_inputs,
)


class TestMutationDetection:
    """Test mutation detection for various types."""

    def test_dataframe_mutation_detection(self):
        """Test detecting DataFrame mutation via inplace operation."""
        # Create a DataFrame
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})

        # Create namespace with the DataFrame
        namespace = {"df": df}

        # Take snapshot
        snapshots = snapshot_inputs(namespace, ["df"])
        assert len(snapshots) == 1
        assert snapshots[0].var_name == "df"
        assert snapshots[0].content_hash is not None

        # Mutate the DataFrame
        df.drop("a", axis=1, inplace=True)

        # Detect mutations
        warnings = detect_mutations(namespace, snapshots)

        # Should detect the mutation
        assert len(warnings) > 0
        assert "mutated" in warnings[0]["message"].lower()

    def test_no_mutation_on_reassignment(self):
        """Test that reassignment is not detected as mutation."""
        df = pd.DataFrame({"a": [1, 2, 3]})
        namespace = {"df": df}

        # Take snapshot
        snapshots = snapshot_inputs(namespace, ["df"])
        original_id = snapshots[0].identity

        # Reassign the variable
        namespace["df"] = df.drop("a", axis=1)

        # The new DataFrame has a different id
        assert id(namespace["df"]) != original_id

        # Should NOT detect mutation (identity changed)
        warnings = detect_mutations(namespace, snapshots)
        assert len(warnings) == 0

    def test_no_mutation_on_read_only_access(self):
        """Test that read-only operations don't trigger warnings."""
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
        namespace = {"df": df}

        # Take snapshot
        snapshots = snapshot_inputs(namespace, ["df"])

        # Read-only operations (these don't mutate)
        _ = df.describe()
        _ = df.shape
        _ = df.loc[0]

        # Should NOT detect mutations
        warnings = detect_mutations(namespace, snapshots)
        assert len(warnings) == 0

    def test_snapshot_multiple_inputs(self):
        """Test snapshotting multiple input variables."""
        df1 = pd.DataFrame({"x": [1, 2]})
        df2 = pd.DataFrame({"y": [3, 4]})
        scalar = 42

        namespace = {"df1": df1, "df2": df2, "scalar": scalar}

        # Take snapshot of all inputs
        snapshots = snapshot_inputs(namespace, ["df1", "df2", "scalar"])

        assert len(snapshots) == 3
        assert snapshots[0].var_name == "df1"
        assert snapshots[1].var_name == "df2"
        assert snapshots[2].var_name == "scalar"

    def test_deleted_input_detected(self):
        """Test that deleted input is detected as mutation."""
        df = pd.DataFrame({"a": [1, 2, 3]})
        namespace = {"df": df}

        # Take snapshot
        snapshots = snapshot_inputs(namespace, ["df"])

        # Delete the variable
        del namespace["df"]

        # detect_mutations flags deleted variables as mutations
        warnings = detect_mutations(namespace, snapshots)
        assert isinstance(warnings, list)
        assert len(warnings) == 1, "Expected one warning for deleted variable"
        assert "df" in warnings[0]["var_name"]

    def test_defensive_copy_arrow(self):
        """Test defensive copy for arrow content type."""
        df = pd.DataFrame({"a": [1, 2, 3]})

        # Arrow IPC doesn't need a copy (deserialization produces new object)
        copy_df = apply_defensive_copy(df, "arrow/ipc")

        # For arrow, we return the same object (no copy needed)
        assert copy_df is df

    def test_defensive_copy_json(self):
        """Test defensive copy for JSON content type."""
        data = {"key": "value", "list": [1, 2, 3]}

        # JSON objects get shallow copy
        copy_data = apply_defensive_copy(data, "json/object")

        # Should be a different object
        assert copy_data is not data

        # But shallow copy means nested objects are shared
        assert copy_data == data
        assert copy_data["list"] is data["list"]

    def test_defensive_copy_pickle(self):
        """Test defensive copy for pickle content type."""

        class CustomClass:
            def __init__(self, value):
                self.value = value

        obj = CustomClass(42)

        # Pickle objects get deep copy
        copy_obj = apply_defensive_copy(obj, "pickle/object")

        # Should be a different object
        assert copy_obj is not obj

        # But values should be equal
        assert copy_obj.value == obj.value


class TestInputSnapshot:
    """Test InputSnapshot dataclass."""

    def test_snapshot_creation(self):
        """Test creating an InputSnapshot."""
        snapshot = InputSnapshot(
            var_name="test_var",
            identity=12345,
            content_hash="abc123",
        )

        assert snapshot.var_name == "test_var"
        assert snapshot.identity == 12345
        assert snapshot.content_hash == "abc123"

    def test_snapshot_with_none_hash(self):
        """Test snapshot with None content hash."""
        snapshot = InputSnapshot(
            var_name="test_var",
            identity=12345,
            content_hash=None,
        )

        assert snapshot.content_hash is None
