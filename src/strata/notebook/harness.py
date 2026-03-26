"""Harness script that runs inside the notebook subprocess.

This script receives a manifest JSON file (path as argv[1]), executes cell source,
captures stdout/stderr, and serializes outputs.

It runs in the notebook's venv, so it only has access to the notebook's dependencies.
It cannot import from strata — instead, it includes its own serialization logic.

TODO(tech-debt): Serialization logic is duplicated across harness.py,
pool_worker.py, serializer.py, and inspect_repl.py. Consider bundling
serializer.py into the subprocess environment to deduplicate.
"""

from __future__ import annotations

import io
import json
import sys
import traceback
from pathlib import Path
from typing import Any


def load_manifest(manifest_path: str) -> dict:
    """Load the execution manifest.

    Args:
        manifest_path: Path to manifest.json

    Returns:
        Manifest dict with: source, inputs, output_dir
    """
    with open(manifest_path) as f:
        return json.load(f)


def get_serializer_module() -> Any:
    """Get or create the serializer module.

    Since we can't import from strata, we define serialization here.

    Returns:
        A module-like object with serialize_value() function
    """

    class Serializer:
        @staticmethod
        def serialize_value(value, output_dir, variable_name):
            """Serialize a value — inline implementation."""
            from pathlib import Path

            output_dir = Path(output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)

            # Detect content type
            content_type = Serializer._detect_content_type(value)

            if content_type == "arrow/ipc":
                try:
                    return Serializer._serialize_arrow(
                        value, output_dir, variable_name
                    )
                except (ImportError, ValueError):
                    # pyarrow not available — fall back to JSON for DataFrames
                    return Serializer._serialize_dataframe_json(
                        value, output_dir, variable_name
                    )
            elif content_type == "json/object":
                return Serializer._serialize_json(
                    value, output_dir, variable_name
                )
            elif content_type == "module/import":
                return Serializer._serialize_module(
                    value, output_dir, variable_name
                )
            else:
                return Serializer._serialize_pickle(
                    value, output_dir, variable_name
                )

        @staticmethod
        def _detect_content_type(value):
            """Detect content type."""
            try:
                import pyarrow as pa

                if isinstance(value, (pa.Table, pa.RecordBatch)):
                    return "arrow/ipc"
            except Exception:
                pass

            try:
                import pandas as pd

                if isinstance(value, (pd.DataFrame, pd.Series)):
                    return "arrow/ipc"
            except Exception:
                pass

            try:
                import numpy as np

                if isinstance(value, np.ndarray):
                    return "arrow/ipc"
            except Exception:
                pass

            # JSON-safe types
            if isinstance(value, (dict, list, int, float, str, bool, type(None))):
                try:
                    import json

                    json.dumps(value)
                    return "json/object"
                except (TypeError, ValueError):
                    pass

            # Module objects can't be pickled — use special type
            import types
            if isinstance(value, types.ModuleType):
                return "module/import"

            return "pickle/object"

        @staticmethod
        def _serialize_arrow(value, output_dir, variable_name):
            """Serialize to Arrow IPC."""
            import pyarrow as pa

            # Convert to Arrow table
            if isinstance(value, pa.RecordBatch):
                table = pa.Table.from_batches([value])
            elif isinstance(value, pa.Table):
                table = value
            else:
                try:
                    import pandas as pd

                    if isinstance(value, (pd.DataFrame, pd.Series)):
                        table = pa.Table.from_pandas(value)
                    else:
                        import numpy as np

                        if isinstance(value, np.ndarray):
                            table = pa.table({"array": value})
                        else:
                            raise ValueError(
                                f"Cannot convert {type(value)} to Arrow"
                            )
                except ImportError:
                    raise ValueError("pandas/numpy not available")

            # Write Arrow IPC stream
            filename = f"{variable_name}.arrow"
            filepath = output_dir / filename

            with open(filepath, "wb") as f:
                writer = pa.ipc.new_stream(f, table.schema)
                writer.write_table(table)
                writer.close()

            size_bytes = filepath.stat().st_size
            num_rows = table.num_rows
            column_names = table.column_names

            # Generate preview
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

        @staticmethod
        def _serialize_json(value, output_dir, variable_name):
            """Serialize to JSON."""
            import json

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

        @staticmethod
        def _serialize_dataframe_json(value, output_dir, variable_name):
            """Fallback: serialize DataFrame as JSON when pyarrow is unavailable.

            Produces the same metadata shape as _serialize_arrow (columns,
            rows, preview) so the frontend can render it as a table.
            """
            import json

            try:
                import pandas as pd
                if isinstance(value, pd.Series):
                    value = value.to_frame()
                columns = list(value.columns)
                num_rows = len(value)
                # Preview: first 20 rows as list of lists
                preview = value.head(20).values.tolist()
            except Exception:
                columns = []
                num_rows = 0
                preview = []

            filename = f"{variable_name}.json"
            filepath = output_dir / filename

            # Store as JSON records for the harness to read back
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(value.to_dict(orient="list"), f)

            return {
                "content_type": "arrow/ipc",  # frontend renders as table
                "file": filename,
                "rows": num_rows,
                "columns": columns,
                "bytes": filepath.stat().st_size,
                "preview": preview,
            }

        @staticmethod
        def _serialize_module(value, output_dir, variable_name):
            """Serialize a module reference as JSON with the module name."""
            import json

            filename = f"{variable_name}.module.json"
            filepath = output_dir / filename

            module_name = getattr(value, "__name__", variable_name)
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump({"module_name": module_name}, f)

            return {
                "content_type": "module/import",
                "file": filename,
                "bytes": filepath.stat().st_size,
                "preview": f"<module '{module_name}'>",
            }

        @staticmethod
        def _serialize_pickle(value, output_dir, variable_name):
            """Serialize to pickle."""
            import pickle

            filename = f"{variable_name}.pickle"
            filepath = output_dir / filename

            with open(filepath, "wb") as f:
                pickle.dump(value, f, protocol=5)

            size_bytes = filepath.stat().st_size
            type_name = type(value).__name__

            return {
                "content_type": "pickle/object",
                "file": filename,
                "bytes": size_bytes,
                "type": type_name,
                "preview": f"<{type_name} object>",
            }

    return Serializer


def deserialize_inputs(manifest: dict) -> dict[str, Any]:
    """Deserialize input variables from manifest.

    The manifest has an 'inputs' section with format:
    {
        "var_name": {
            "content_type": "arrow/ipc" | "json/object" | "pickle/object",
            "file": "/tmp/path/to/var_name.arrow"  # Path relative to output_dir
        },
        ...
    }

    Returns:
        Dict of variable_name -> deserialized value
    """
    from pathlib import Path

    inputs = {}
    output_dir = Path(manifest.get("output_dir", "/tmp/strata_output"))
    input_specs = manifest.get("inputs", {})

    for var_name, spec in input_specs.items():
        content_type = spec.get("content_type", "")
        file_path = spec.get("file", "")

        if not file_path:
            print(f"Warning: No file path for input {var_name}")
            continue

        # Resolve file path relative to output_dir
        full_path = output_dir / file_path

        if not full_path.exists():
            print(f"Warning: Input file not found: {full_path}")
            continue

        try:
            if content_type == "arrow/ipc":
                inputs[var_name] = _deserialize_arrow(full_path)
            elif content_type == "json/object":
                inputs[var_name] = _deserialize_json(full_path)
            elif content_type == "pickle/object":
                inputs[var_name] = _deserialize_pickle(full_path)
            elif content_type == "module/import":
                inputs[var_name] = _deserialize_module(full_path)
            else:
                print(f"Warning: Unknown content type for {var_name}: {content_type}")
        except Exception as e:
            print(f"Error deserializing {var_name}: {e}")
            continue

    return inputs


def _deserialize_arrow(file_path) -> Any:
    """Deserialize Arrow IPC file."""
    from pathlib import Path

    import pyarrow as pa

    file_path = Path(file_path)
    with open(file_path, "rb") as f:
        reader = pa.ipc.open_stream(f)
        table = reader.read_all()
    return table


def _deserialize_json(file_path) -> Any:
    """Deserialize JSON file."""
    import json
    from pathlib import Path

    file_path = Path(file_path)
    with open(file_path, encoding="utf-8") as f:
        return json.load(f)


def _deserialize_pickle(file_path) -> Any:
    """Deserialize pickle file."""
    import pickle
    from pathlib import Path

    file_path = Path(file_path)
    with open(file_path, "rb") as f:
        return pickle.load(f)


def _deserialize_module(file_path) -> Any:
    """Deserialize a module reference by re-importing it."""
    import importlib
    import json
    from pathlib import Path

    file_path = Path(file_path)
    with open(file_path, encoding="utf-8") as f:
        data = json.load(f)
    module_name = data["module_name"]
    return importlib.import_module(module_name)


def snapshot_inputs(namespace: dict, input_names: list[str]) -> list[dict]:
    """Take snapshots of input variables before execution (M6).

    Args:
        namespace: Namespace dict
        input_names: List of input variable names

    Returns:
        List of {var_name, identity, content_hash}
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

        snapshots.append({
            "var_name": var_name,
            "identity": var_id,
            "content_hash": content_hash,
        })

    return snapshots


def _hash_dataframe_sample(df) -> str:
    """Hash first 5 + last 5 rows of DataFrame (M6)."""
    import hashlib

    h = hashlib.sha256()

    try:
        h.update(str(df.shape).encode())
        try:
            h.update(str(df.dtypes.to_dict()).encode())
        except AttributeError:
            h.update(str(df.dtype).encode())

        head_json = df.head(5).to_json()
        h.update(head_json.encode())

        if len(df) > 5:
            tail_json = df.tail(5).to_json()
            h.update(tail_json.encode())

        return h.hexdigest()
    except Exception:
        return ""


def detect_mutations(namespace: dict, snapshots: list[dict]) -> list[dict]:
    """Detect mutations by comparing current state (M6).

    Args:
        namespace: Current namespace
        snapshots: Snapshots from before execution

    Returns:
        List of {var_name, message, suggestion}
    """
    warnings = []

    for snapshot in snapshots:
        var_name = snapshot["var_name"]

        if var_name not in namespace:
            continue

        current_value = namespace[var_name]
        current_id = id(current_value)

        # If identity changed, it was reassigned (not a mutation)
        if current_id != snapshot["identity"]:
            continue

        # Same identity — check if mutated
        try:
            import pandas as pd

            if isinstance(current_value, (pd.DataFrame, pd.Series)):
                if snapshot.get("content_hash"):
                    current_hash = _hash_dataframe_sample(current_value)
                    if current_hash != snapshot["content_hash"]:
                        warnings.append({
                            "var_name": var_name,
                            "message": f"'{var_name}' was mutated without reassignment",
                            "suggestion": (
                                "Consider using df = df.copy() or "
                                "df = df.drop(...) instead of inplace=True"
                            ),
                        })
        except ImportError:
            pass

    return warnings


def _exec_with_display(source: str, namespace: dict) -> Any | None:
    """Execute source and return the last expression's value (if any).

    If the last statement is a bare expression (``ast.Expr``), it is
    compiled separately with ``eval`` so we can capture its value —
    mimicking Jupyter / IPython's ``Out[n]`` behavior.  ``None`` results
    are suppressed (matching IPython).

    All other statements are executed via ``exec`` as usual.
    """
    import ast as _ast

    try:
        tree = _ast.parse(source)
    except SyntaxError:
        # Fall back to plain exec on parse failure
        exec(source, namespace)  # noqa: S102
        return None

    if not tree.body:
        return None

    last = tree.body[-1]

    if isinstance(last, _ast.Expr):
        # Split: exec everything except the last, then eval the last
        if len(tree.body) > 1:
            mod = _ast.Module(body=tree.body[:-1], type_ignores=[])
            _ast.fix_missing_locations(mod)
            exec(compile(mod, "<cell>", "exec"), namespace)  # noqa: S102

        expr = _ast.Expression(body=last.value)
        _ast.fix_missing_locations(expr)
        result = eval(compile(expr, "<cell>", "eval"), namespace)  # noqa: S307
        return result if result is not None else None
    else:
        # Last statement is not an expression — plain exec
        exec(source, namespace)  # noqa: S102
        return None


def execute_cell(source: str, inputs: dict) -> tuple[dict, str, str, list[dict]]:
    """Execute a cell and capture outputs.

    Args:
        source: Cell source code
        inputs: Input variables (dict of name -> value)

    Returns:
        Tuple of (outputs_dict, stdout_str, stderr_str, mutation_warnings)
    """
    # Create namespace with inputs
    namespace = dict(inputs)

    # Capture stdout/stderr
    old_stdout = sys.stdout
    old_stderr = sys.stderr
    stdout_capture = io.StringIO()
    stderr_capture = io.StringIO()

    try:
        sys.stdout = stdout_capture
        sys.stderr = stderr_capture

        # Record what was in the namespace before execution
        namespace_before = set(namespace.keys())
        input_names = list(namespace.keys())

        # Snapshot input identities so we can detect reassignment
        input_identities = {name: id(namespace[name]) for name in input_names}

        # M6: Take snapshots of input variables
        input_snapshots = snapshot_inputs(namespace, input_names)

        # Execute the cell.  If the last statement is a bare expression
        # (e.g. ``x``, ``df.head()``, ``1 + 2``), eval it separately and
        # capture the result as ``_`` — like Jupyter / IPython.
        _display_value = _exec_with_display(source, namespace)

        # Find new and reassigned variables
        _skip = {"__builtins__", "__name__", "__doc__", "__package__"}
        new_vars = {}
        for name, value in namespace.items():
            if name.startswith("_") or name in _skip:
                continue
            if name not in namespace_before:
                # Truly new variable
                new_vars[name] = value
            elif id(value) != input_identities.get(name):
                # Input variable was reassigned (e.g. x = x + 1)
                new_vars[name] = value

        # Include last-expression display value (like Jupyter's Out[n])
        if _display_value is not None:
            new_vars["_"] = _display_value

        # M6: Detect mutations on input variables
        mutation_warnings = detect_mutations(namespace, input_snapshots)

        return (
            new_vars,
            stdout_capture.getvalue(),
            stderr_capture.getvalue(),
            mutation_warnings,
        )

    finally:
        sys.stdout = old_stdout
        sys.stderr = old_stderr


def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print("Usage: harness.py <manifest_path>", file=sys.stderr)
        sys.exit(1)

    manifest_path = sys.argv[1]
    manifest: dict = {}

    try:
        # Load manifest
        manifest = load_manifest(manifest_path)
        source = manifest.get("source", "")
        output_dir = Path(manifest.get("output_dir", "/tmp/strata_output"))

        # Deserialize inputs from manifest
        inputs = deserialize_inputs(manifest)

        # Execute cell
        outputs, stdout_text, stderr_text, mutation_warnings = execute_cell(source, inputs)

        # Serialize outputs
        serializer = get_serializer_module()
        serialized_outputs = {}
        for var_name, value in outputs.items():
            try:
                output_meta = serializer.serialize_value(
                    value, output_dir, var_name
                )
                serialized_outputs[var_name] = output_meta
            except Exception as e:
                # If serialization fails, record the error
                serialized_outputs[var_name] = {
                    "error": str(e),
                    "type": type(value).__name__,
                }

        # Write result manifest
        result = {
            "success": True,
            "variables": serialized_outputs,
            "stdout": stdout_text,
            "stderr": stderr_text,
            "mutation_warnings": mutation_warnings,
        }

        result_path = output_dir / "manifest.json"
        result_path.parent.mkdir(parents=True, exist_ok=True)
        with open(result_path, "w") as f:
            json.dump(result, f, indent=2)

    except Exception as e:
        # Write error result
        error_result = {
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc(),
            "variables": {},
            "stdout": stdout_text if "stdout_text" in locals() else "",
            "stderr": stderr_text if "stderr_text" in locals() else "",
            "mutation_warnings": [],
        }

        output_dir = Path(manifest.get("output_dir", "/tmp/strata_output"))
        result_path = output_dir / "manifest.json"
        result_path.parent.mkdir(parents=True, exist_ok=True)
        with open(result_path, "w") as f:
            json.dump(error_result, f, indent=2)

        sys.exit(1)


if __name__ == "__main__":
    main()
