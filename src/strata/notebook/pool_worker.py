#!/usr/bin/env python3
"""Pool worker script that runs in the notebook subprocess.

TODO(tech-debt): Serialization logic is duplicated across pool_worker.py,
harness.py, serializer.py, and inspect_repl.py. The harness/pool_worker
run in the notebook's venv and cannot import strata, so the code is inlined.
Consider bundling serializer.py into the subprocess environment.

This script:
1. Parses pyproject.toml to find common deps
2. Imports them to warm up the process
3. Sends a 'ready' signal
4. Waits for a manifest path on stdin
5. Runs the harness logic (inline)
6. Outputs result and exits

It runs in the notebook's venv, so it can only import from the notebook's dependencies.
It cannot import from strata.
"""

import json
import sys
from pathlib import Path
from typing import Any


def parse_common_imports() -> list[str]:
    """Parse pyproject.toml to find common imports.

    Returns a list of module names to import for warming.
    """
    try:
        import tomllib
    except ImportError:
        import tomli as tomllib  # type: ignore

    try:
        notebook_dir = Path(sys.argv[1])
        pyproject_path = notebook_dir / "pyproject.toml"

        if not pyproject_path.exists():
            return []

        with open(pyproject_path, "rb") as f:
            data = tomllib.load(f)

        # Extract dependencies from [project] section
        dependencies = data.get("project", {}).get("dependencies", [])

        # Parse package names from requirements strings
        common_imports = []
        for dep in dependencies:
            # Remove version specifiers and extras
            package_name = dep.split("[")[0].split(";")[0].split(">=")[
                0
            ].split("==")[0].split("<")[0].split(">")[0].strip()

            # Normalize package name to import name
            # (e.g., "pandas" -> "pandas", "pyarrow" -> "pyarrow")
            # Some packages have different import names, but we'll try the most common ones
            common_imports.append(package_name)

        return common_imports
    except Exception:
        return []


def warm_imports(imports: list[str]) -> None:
    """Import common packages to warm up the process.

    Args:
        imports: List of module names to import
    """
    for module_name in imports:
        try:
            __import__(module_name)
        except (ImportError, ModuleNotFoundError):
            # Skip modules that aren't installed
            pass


def load_manifest(manifest_path: str) -> dict:
    """Load the execution manifest.

    Args:
        manifest_path: Path to manifest.json

    Returns:
        Manifest dict with: source, inputs, output_dir
    """
    with open(manifest_path) as f:
        return json.load(f)


def execute_harness(manifest: dict) -> dict:
    """Execute cell using harness logic.

    This is a simplified version of the full harness that avoids
    reimporting the entire harness module. We inline the key logic here.

    Args:
        manifest: Manifest dict with source, inputs, output_dir

    Returns:
        Result dict with outputs, error, etc.
    """
    import io
    import traceback

    source = manifest.get("source", "")
    inputs = manifest.get("inputs", {})
    output_dir = Path(manifest.get("output_dir", ""))

    # Create namespace
    namespace: dict[str, Any] = {}

    # Inject inputs (inputs is a dict: {var_name: {content_type, file}})
    for var_name, input_spec in inputs.items():
        if input_spec.get("content_type") == "arrow/ipc":
            try:
                import pyarrow as pa

                arrow_file = output_dir / input_spec.get("file", "")
                if arrow_file.exists():
                    with open(arrow_file, "rb") as f:
                        reader = pa.ipc.open_stream(f)
                        table = reader.read_all()
                        try:
                            import pandas as pd  # noqa: F401

                            namespace[var_name] = table.to_pandas()
                        except ImportError:
                            namespace[var_name] = table
            except Exception:
                pass
        elif input_spec.get("content_type") == "json/object":
            try:
                json_file = output_dir / input_spec.get("file", "")
                if json_file.exists():
                    with open(json_file) as f:
                        namespace[var_name] = json.load(f)
            except Exception:
                pass
        elif input_spec.get("content_type") == "pickle/object":
            try:
                import pickle

                pkl_file = output_dir / input_spec.get("file", "")
                if pkl_file.exists():
                    with open(pkl_file, "rb") as f:
                        namespace[var_name] = pickle.load(f)
            except Exception:
                pass

    # Record namespace before execution for filtering outputs
    namespace_before = set(namespace.keys())
    input_identities = {name: id(namespace[name]) for name in namespace_before}

    # Capture stdout/stderr
    old_stdout = sys.stdout
    old_stderr = sys.stderr
    stdout_buffer = io.StringIO()
    stderr_buffer = io.StringIO()
    sys.stdout = stdout_buffer
    sys.stderr = stderr_buffer

    _skip = {"__builtins__", "__name__", "__doc__", "__package__"}

    try:
        # Execute cell
        exec(source, namespace)

        # Restore stdout/stderr
        sys.stdout = old_stdout
        sys.stderr = old_stderr

        # Collect new and reassigned outputs (exclude unchanged inputs)
        outputs = {}
        for key, value in namespace.items():
            if key.startswith("_") or key in _skip:
                continue
            if key not in namespace_before:
                # Truly new variable
                try:
                    outputs[key] = _serialize_value(value, output_dir, key)
                except Exception as e:
                    outputs[key] = {
                        "content_type": "error",
                        "error": str(e),
                    }
            elif id(value) != input_identities.get(key):
                # Input variable was reassigned
                try:
                    outputs[key] = _serialize_value(value, output_dir, key)
                except Exception as e:
                    outputs[key] = {
                        "content_type": "error",
                        "error": str(e),
                    }

        return {
            "success": True,
            "outputs": outputs,
            "stdout": stdout_buffer.getvalue(),
            "stderr": stderr_buffer.getvalue(),
            "error": None,
        }

    except Exception as e:
        sys.stdout = old_stdout
        sys.stderr = old_stderr

        return {
            "success": False,
            "outputs": {},
            "stdout": stdout_buffer.getvalue(),
            "stderr": stderr_buffer.getvalue(),
            "error": f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}",
        }


def _serialize_value(value: Any, output_dir: Path, variable_name: str) -> dict:
    """Serialize a value to a file and return metadata.

    Args:
        value: The value to serialize
        output_dir: Directory to write the file
        variable_name: Name of the variable

    Returns:
        Metadata dict
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # Detect content type
    content_type = _detect_content_type(value)

    if content_type == "arrow/ipc":
        return _serialize_arrow(value, output_dir, variable_name)
    elif content_type == "json/object":
        return _serialize_json(value, output_dir, variable_name)
    else:
        return _serialize_pickle(value, output_dir, variable_name)


def _detect_content_type(value: Any) -> str:
    """Detect content type."""
    try:
        import pandas as pd
        import pyarrow as pa

        if isinstance(
            value, (pa.Table, pa.RecordBatch, pd.DataFrame, pd.Series)
        ):
            return "arrow/ipc"
    except ImportError:
        pass

    try:
        import numpy as np

        if isinstance(value, np.ndarray):
            return "arrow/ipc"
    except ImportError:
        pass

    if isinstance(value, (dict, list, int, float, str, bool, type(None))):
        try:
            json.dumps(value)
            return "json/object"
        except (TypeError, ValueError):
            pass

    return "pickle/object"


def _serialize_arrow(value: Any, output_dir: Path, variable_name: str) -> dict:
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


def _serialize_json(value: Any, output_dir: Path, variable_name: str) -> dict:
    """Serialize to JSON."""
    filename = f"{variable_name}.json"
    filepath = output_dir / filename

    with open(filepath, "w") as f:
        json.dump(value, f)

    size_bytes = filepath.stat().st_size

    return {
        "content_type": "json/object",
        "file": filename,
        "bytes": size_bytes,
        "preview": value,
    }


def _serialize_pickle(
    value: Any, output_dir: Path, variable_name: str
) -> dict:
    """Serialize to pickle."""
    import pickle

    filename = f"{variable_name}.pickle"
    filepath = output_dir / filename

    with open(filepath, "wb") as f:
        pickle.dump(value, f)

    size_bytes = filepath.stat().st_size

    # For pickle, we can't easily generate a preview
    return {
        "content_type": "pickle/object",
        "file": filename,
        "bytes": size_bytes,
        "preview": f"<{type(value).__name__} object>",
    }


def main() -> None:
    """Main entry point for the pool worker."""
    try:
        # Parse and warm imports
        imports = parse_common_imports()
        warm_imports(imports)

        # Send ready signal
        print("ready", flush=True)

        # Wait for manifest path on stdin
        while True:
            manifest_path_line = sys.stdin.readline()
            if not manifest_path_line:
                break

            manifest_path = manifest_path_line.strip()
            if not manifest_path:
                continue

            try:
                # Load and execute manifest
                manifest = load_manifest(manifest_path)
                result = execute_harness(manifest)

                # Output result as JSON and flush
                print(json.dumps(result), flush=True)

            except Exception as e:
                error_result = {
                    "success": False,
                    "outputs": {},
                    "stdout": "",
                    "stderr": "",
                    "error": f"Pool worker error: {str(e)}",
                }
                print(json.dumps(error_result), flush=True)

    except Exception as e:
        print(f"fatal: {e}", file=sys.stderr, flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
