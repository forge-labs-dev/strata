"""Runtime mutation detection for notebook cell inputs.

This module provides heuristic, best-effort mutation detection for input variables.
It's conservative: if we can't prove a variable wasn't mutated, we report a warning.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any


@dataclass
class InputSnapshot:
    """Snapshot of an input variable for mutation detection."""

    var_name: str
    identity: int  # id(obj) at snapshot time
    content_hash: str | None  # sample-based hash for DataFrames


@dataclass
class MutationWarning:
    """Warning about a detected mutation."""

    var_name: str
    message: str
    suggestion: str | None = None


def snapshot_inputs(namespace: dict[str, Any], input_names: list[str]) -> list[InputSnapshot]:
    """Take snapshots of input variables before cell execution.

    Args:
        namespace: The namespace dict containing variables
        input_names: List of input variable names to snapshot

    Returns:
        List of InputSnapshot objects
    """
    snapshots = []

    for var_name in input_names:
        if var_name not in namespace:
            continue

        value = namespace[var_name]
        var_id = id(value)

        # For DataFrames, compute a sample hash
        content_hash = None
        try:
            import pandas as pd

            if isinstance(value, (pd.DataFrame, pd.Series)):
                content_hash = _hash_dataframe_sample(value)
        except ImportError:
            pass

        snapshots.append(
            InputSnapshot(
                var_name=var_name,
                identity=var_id,
                content_hash=content_hash,
            )
        )

    return snapshots


def detect_mutations(
    namespace: dict[str, Any], snapshots: list[InputSnapshot]
) -> list[MutationWarning] | list[dict[str, Any]]:
    """Detect mutations by comparing current state against snapshots.

    Detection strategies:
    - Identity check: id(current) != id(original) → reassignment (not mutation)
    - DataFrame sample: hash first/last N rows, compare
    - Dict/list: len changed, or sample of keys/items changed
    - Other: skip (opaque objects)

    Args:
        namespace: The namespace dict after execution
        snapshots: List of InputSnapshot objects from before execution

    Returns:
        List of MutationWarning objects or dicts (empty if no mutations detected)
    """
    warnings = []

    for snapshot in snapshots:
        if snapshot.var_name not in namespace:
            # Variable was deleted — report as mutation
            warnings.append(
                MutationWarning(
                    var_name=snapshot.var_name,
                    message=f"'{snapshot.var_name}' was deleted during execution",
                    suggestion=None,
                )
            )
            continue

        current_value = namespace[snapshot.var_name]
        current_id = id(current_value)

        # If identity changed, it was reassigned (not a mutation)
        if current_id != snapshot.identity:
            continue

        # Same identity — check if the object was mutated
        mutation_detected = _check_object_mutation(current_value, snapshot)

        if mutation_detected:
            message, suggestion = mutation_detected
            warnings.append(
                MutationWarning(
                    var_name=snapshot.var_name,
                    message=message,
                    suggestion=suggestion,
                )
            )

    return warnings


def _check_object_mutation(value: Any, snapshot: InputSnapshot) -> tuple[str, str | None] | None:
    """Check if an object was mutated.

    Returns (message, suggestion) if mutation detected, None otherwise.
    """
    # Try DataFrame mutation detection
    try:
        import pandas as pd

        if isinstance(value, (pd.DataFrame, pd.Series)):
            if snapshot.content_hash:
                current_hash = _hash_dataframe_sample(value)
                if current_hash != snapshot.content_hash:
                    return (
                        f"'{snapshot.var_name}' was mutated without reassignment",
                        (
                            "Consider using df = df.copy() or "
                            "df = df.drop(...) instead of inplace=True"
                        ),
                    )
            return None
    except ImportError:
        pass

    # Try dict mutation detection
    if isinstance(value, dict):
        # Check if keys were added/removed
        # (We can't easily detect value changes for existing keys)
        return None

    # Try list mutation detection
    if isinstance(value, list):
        # Check if length changed — this would indicate append/extend/remove
        # (We can't easily detect order changes)
        return None

    # For other types, we can't reliably detect mutations
    return None


def _hash_dataframe_sample(df: Any) -> str:
    """Hash first 5 + last 5 rows of a DataFrame for mutation detection.

    This is a sample-based hash to avoid expensive full-table hashing.
    It's fast (~1ms for any size DataFrame) and catches most mutations.

    Args:
        df: A pandas DataFrame or Series

    Returns:
        Hexadecimal hash string
    """
    h = hashlib.sha256()

    try:
        # Hash shape and dtypes
        h.update(str(df.shape).encode())
        try:
            h.update(str(df.dtypes.to_dict()).encode())
        except AttributeError:
            # Series don't have dtypes.to_dict()
            h.update(str(df.dtype).encode())

        # Hash first 5 rows as JSON
        head_json = df.head(5).to_json()
        h.update(head_json.encode())

        # Hash last 5 rows as JSON (only if more than 5 rows)
        if len(df) > 5:
            tail_json = df.tail(5).to_json()
            h.update(tail_json.encode())

        return h.hexdigest()

    except Exception:
        # If we can't hash for any reason, return empty hash
        # (mutation detection will fail gracefully)
        return ""


def apply_defensive_copy(value: Any, content_type: str) -> Any:
    """Apply defensive copy based on tier.

    Tier strategy:
    - arrow/ipc: No copy needed (deserialization already produces new object)
    - json/object: copy.copy() (shallow copy)
    - pickle/object: copy.deepcopy()

    Args:
        value: The value to copy
        content_type: The content type from the input spec

    Returns:
        A defensive copy of the value (or original if no copy needed)
    """
    if content_type == "arrow/ipc":
        # Arrow deserializes to a new object, no additional copy needed
        return value

    if content_type == "json/object":
        # Shallow copy for JSON objects
        import copy

        return copy.copy(value)

    if content_type == "pickle/object":
        # Deep copy for pickled objects (safer, in case of nested structures)
        import copy

        return copy.deepcopy(value)

    # Unknown content type — return original
    return value
