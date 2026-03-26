"""Three-tier serialization for notebook cell outputs.

Tier 1: Arrow IPC (for DataFrames, Tables, RecordBatches, numpy arrays)
Tier 2: JSON (for dicts, lists, scalars, strings, numbers, bools, None)
Tier 3: Pickle (fallback for everything else — models, custom objects)

This module can be used both in the harness subprocess and in the server.
"""

from __future__ import annotations

import json
import pickle
from pathlib import Path
from typing import Any


def detect_content_type(value: Any) -> str:
    """Detect the appropriate content type for a value.

    Args:
        value: The value to serialize

    Returns:
        Content type string: "arrow/ipc", "json/object", or "pickle/object"
    """
    # Try to import pandas and pyarrow, but don't fail if unavailable
    try:
        import pandas as pd
        import pyarrow as pa

        if isinstance(
            value, (pa.Table, pa.RecordBatch, pd.DataFrame, pd.Series)
        ):
            return "arrow/ipc"
    except ImportError:
        pass

    # Try numpy arrays
    try:
        import numpy as np

        if isinstance(value, np.ndarray):
            return "arrow/ipc"
    except ImportError:
        pass

    # JSON-safe types
    if isinstance(value, (dict, list, int, float, str, bool, type(None))):
        if is_json_safe(value):
            return "json/object"

    # Fall back to pickle
    return "pickle/object"


def is_json_safe(value: Any) -> bool:
    """Check if a value is JSON-serializable.

    Args:
        value: The value to check

    Returns:
        True if the value can be JSON serialized, False otherwise
    """
    try:
        json.dumps(value)
        return True
    except (TypeError, ValueError):
        return False


def serialize_value(
    value: Any, output_dir: Path, variable_name: str
) -> dict[str, Any]:
    """Serialize a value to a file and return metadata.

    Args:
        value: The value to serialize
        output_dir: Directory to write serialized data to
        variable_name: Name of the variable (used for filename)

    Returns:
        Dictionary with: content_type, file, rows (for tables), columns, bytes
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    content_type = detect_content_type(value)

    if content_type == "arrow/ipc":
        return _serialize_arrow(value, output_dir, variable_name)
    elif content_type == "json/object":
        return _serialize_json(value, output_dir, variable_name)
    else:  # pickle/object
        return _serialize_pickle(value, output_dir, variable_name)


def _serialize_arrow(
    value: Any, output_dir: Path, variable_name: str
) -> dict[str, Any]:
    """Serialize to Arrow IPC format.

    Args:
        value: DataFrame, Table, RecordBatch, or numpy array
        output_dir: Output directory
        variable_name: Variable name for filename

    Returns:
        Metadata dict with Arrow-specific fields
    """
    import pyarrow as pa

    # Convert to Arrow table if needed
    if isinstance(value, pa.RecordBatch):
        table = pa.table(value)
    elif isinstance(value, pa.Table):
        table = value
    else:
        # Try pandas DataFrame
        try:
            import pandas as pd

            if isinstance(value, (pd.DataFrame, pd.Series)):
                table = pa.Table.from_pandas(value)
            else:
                # numpy array
                import numpy as np

                if isinstance(value, np.ndarray):
                    table = pa.table({"array": value})
                else:
                    raise ValueError(f"Cannot convert {type(value)} to Arrow table")
        except ImportError:
            raise ValueError("pandas/numpy not available for Arrow conversion")

    # Write Arrow IPC
    filename = f"{variable_name}.arrow"
    filepath = output_dir / filename

    sink = pa.BufferOutputStream()
    writer = pa.ipc.RecordBatchStreamWriter(sink, table.schema)
    for i in range(table.num_rows // 8192 + 1):
        start = i * 8192
        end = min(start + 8192, table.num_rows)
        if start < table.num_rows:
            batch = table.slice(start, end - start).to_batches()[0]
            writer.write_batch(batch)
    writer.close()

    # Write to file
    with open(filepath, "wb") as f:
        f.write(sink.getvalue().to_pybytes())

    # Get metadata
    size_bytes = filepath.stat().st_size
    num_rows = table.num_rows
    column_names = table.column_names

    # Generate preview (first 20 rows as list of lists)
    preview = []
    for i in range(min(20, num_rows)):
        row = [col[i].as_py() for col in table.columns]
        preview.append(row)

    return {
        "content_type": "arrow/ipc",
        "file": filename,
        "rows": num_rows,
        "columns": column_names,
        "bytes": size_bytes,
        "preview": preview,
    }


def _serialize_json(
    value: Any, output_dir: Path, variable_name: str
) -> dict[str, Any]:
    """Serialize to JSON format.

    Args:
        value: JSON-serializable value
        output_dir: Output directory
        variable_name: Variable name for filename

    Returns:
        Metadata dict with JSON-specific fields
    """
    filename = f"{variable_name}.json"
    filepath = output_dir / filename

    json_str = json.dumps(value, indent=2)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(json_str)

    size_bytes = filepath.stat().st_size

    return {
        "content_type": "json/object",
        "file": filename,
        "bytes": size_bytes,
        "preview": value,
    }


def _serialize_pickle(
    value: Any, output_dir: Path, variable_name: str
) -> dict[str, Any]:
    """Serialize to pickle format.

    Args:
        value: Any Python object
        output_dir: Output directory
        variable_name: Variable name for filename

    Returns:
        Metadata dict with pickle-specific fields
    """
    filename = f"{variable_name}.pickle"
    filepath = output_dir / filename
    type_name = type(value).__name__

    try:
        with open(filepath, "wb") as f:
            pickle.dump(value, f, protocol=5)
    except Exception as e:
        return {
            "content_type": "pickle/object",
            "file": None,
            "bytes": 0,
            "type": type_name,
            "preview": f"<{type_name} object>",
            "error": f"Failed to pickle: {e}",
        }

    size_bytes = filepath.stat().st_size

    return {
        "content_type": "pickle/object",
        "file": filename,
        "bytes": size_bytes,
        "type": type_name,
        "preview": f"<{type_name} object>",
    }


def deserialize_value(content_type: str, file_path: Path) -> Any:
    """Deserialize a value from a file.

    Args:
        content_type: The content type of the serialized data
        file_path: Path to the serialized data file

    Returns:
        The deserialized value
    """
    if content_type == "arrow/ipc":
        return _deserialize_arrow(file_path)
    elif content_type == "json/object":
        return _deserialize_json(file_path)
    elif content_type == "pickle/object":
        return _deserialize_pickle(file_path)
    else:
        raise ValueError(f"Unknown content type: {content_type}")


def _deserialize_arrow(file_path: Path) -> Any:
    """Deserialize Arrow IPC data.

    Args:
        file_path: Path to .arrow file

    Returns:
        PyArrow Table
    """
    import pyarrow as pa

    with open(file_path, "rb") as f:
        reader = pa.ipc.open_stream(f)
        table = reader.read_all()
    return table


def _deserialize_json(file_path: Path) -> Any:
    """Deserialize JSON data.

    Args:
        file_path: Path to .json file

    Returns:
        Deserialized Python object (dict, list, scalar, etc.)
    """
    with open(file_path, encoding="utf-8") as f:
        return json.load(f)


def _deserialize_pickle(file_path: Path) -> Any:
    """Deserialize pickle data.

    Args:
        file_path: Path to .pickle file

    Returns:
        Deserialized Python object
    """
    with open(file_path, "rb") as f:
        return pickle.load(f)
