"""Harness script that runs inside the notebook subprocess.

This script receives a manifest JSON file (path as argv[1]), executes cell source,
captures stdout/stderr, and serializes outputs.

It runs in the notebook's venv, so it only has access to the notebook's dependencies.
It cannot ``import strata`` — instead it loads ``serializer.py`` from the same
directory via ``importlib.util``.
"""

from __future__ import annotations

import importlib.util
import io
import json
import sys
import traceback
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Load the shared serializer from the same directory as this script.
# This works even when running inside the notebook's venv because we use
# an absolute file path rather than a package import.
# ---------------------------------------------------------------------------

def _load_serializer():
    _p = Path(__file__).parent / "serializer.py"
    _spec = importlib.util.spec_from_file_location("_nb_serializer", _p)
    _m = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(_m)
    return _m

_ser = _load_serializer()


# ---------------------------------------------------------------------------
# Manifest I/O
# ---------------------------------------------------------------------------


def load_manifest(manifest_path: str) -> dict:
    with open(manifest_path) as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Input deserialization
# ---------------------------------------------------------------------------


def deserialize_inputs(manifest: dict) -> dict[str, Any]:
    """Deserialize input variables listed in the manifest."""
    output_dir = Path(manifest.get("output_dir", "/tmp/strata_output"))
    inputs = {}

    for var_name, spec in manifest.get("inputs", {}).items():
        content_type = spec.get("content_type", "")
        file_name = spec.get("file", "")
        if not file_name:
            print(f"Warning: no file path for input {var_name}", file=sys.stderr)
            continue

        full_path = output_dir / file_name
        if not full_path.exists():
            print(f"Warning: input file not found: {full_path}", file=sys.stderr)
            continue

        try:
            inputs[var_name] = _ser.deserialize_value(content_type, full_path)
        except Exception as e:
            print(f"Error deserializing {var_name}: {e}", file=sys.stderr)

    return inputs


# ---------------------------------------------------------------------------
# Mutation detection helpers (M6)
# ---------------------------------------------------------------------------


def snapshot_inputs(namespace: dict, input_names: list[str]) -> list[dict]:
    snapshots = []
    for var_name in input_names:
        if var_name not in namespace:
            continue
        value = namespace[var_name]
        content_hash = None
        try:
            import pandas as pd
            if isinstance(value, (pd.DataFrame, pd.Series)):
                content_hash = _hash_dataframe_sample(value)
        except ImportError:
            pass
        snapshots.append({
            "var_name": var_name,
            "identity": id(value),
            "content_hash": content_hash,
        })
    return snapshots


def _hash_dataframe_sample(df) -> str:
    import hashlib
    h = hashlib.sha256()
    try:
        h.update(str(df.shape).encode())
        try:
            h.update(str(df.dtypes.to_dict()).encode())
        except AttributeError:
            h.update(str(df.dtype).encode())
        h.update(df.head(5).to_json().encode())
        if len(df) > 5:
            h.update(df.tail(5).to_json().encode())
        return h.hexdigest()
    except Exception:
        return ""


def detect_mutations(namespace: dict, snapshots: list[dict]) -> list[dict]:
    warnings = []
    for snapshot in snapshots:
        var_name = snapshot["var_name"]
        if var_name not in namespace:
            continue
        current = namespace[var_name]
        if id(current) != snapshot["identity"]:
            continue  # reassigned, not mutated
        try:
            import pandas as pd
            if isinstance(current, (pd.DataFrame, pd.Series)):
                if snapshot.get("content_hash"):
                    if _hash_dataframe_sample(current) != snapshot["content_hash"]:
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


# ---------------------------------------------------------------------------
# Cell execution
# ---------------------------------------------------------------------------


def _exec_with_display(source: str, namespace: dict) -> Any | None:
    """Execute source; if the last statement is a bare expression, eval and return it."""
    import ast as _ast

    try:
        tree = _ast.parse(source)
    except SyntaxError:
        exec(source, namespace)  # noqa: S102
        return None

    if not tree.body:
        return None

    last = tree.body[-1]
    if isinstance(last, _ast.Expr):
        if len(tree.body) > 1:
            mod = _ast.Module(body=tree.body[:-1], type_ignores=[])
            _ast.fix_missing_locations(mod)
            exec(compile(mod, "<cell>", "exec"), namespace)  # noqa: S102
        expr = _ast.Expression(body=last.value)
        _ast.fix_missing_locations(expr)
        result = eval(compile(expr, "<cell>", "eval"), namespace)  # noqa: S307
        return result if result is not None else None
    else:
        exec(source, namespace)  # noqa: S102
        return None


def execute_cell(source: str, inputs: dict) -> tuple[dict, str, str, list[dict]]:
    """Execute a cell and return (outputs, stdout, stderr, mutation_warnings)."""
    namespace = dict(inputs)

    old_stdout, old_stderr = sys.stdout, sys.stderr
    stdout_capture = io.StringIO()
    stderr_capture = io.StringIO()

    try:
        sys.stdout = stdout_capture
        sys.stderr = stderr_capture

        namespace_before = set(namespace.keys())
        input_identities = {name: id(namespace[name]) for name in namespace_before}
        input_snapshots = snapshot_inputs(namespace, list(namespace_before))

        _display_value = _exec_with_display(source, namespace)

        _skip = {"__builtins__", "__name__", "__doc__", "__package__"}
        new_vars: dict[str, Any] = {}
        for name, value in namespace.items():
            if name.startswith("_") or name in _skip:
                continue
            if name not in namespace_before or id(value) != input_identities.get(name):
                new_vars[name] = value

        if _display_value is not None:
            new_vars["_"] = _display_value

        mutation_warnings = detect_mutations(namespace, input_snapshots)
        return new_vars, stdout_capture.getvalue(), stderr_capture.getvalue(), mutation_warnings

    finally:
        sys.stdout = old_stdout
        sys.stderr = old_stderr


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main():
    if len(sys.argv) < 2:
        print("Usage: harness.py <manifest_path>", file=sys.stderr)
        sys.exit(1)

    manifest_path = sys.argv[1]
    manifest: dict = {}
    stdout_text = ""
    stderr_text = ""

    try:
        manifest = load_manifest(manifest_path)
        source = manifest.get("source", "")
        output_dir = Path(manifest.get("output_dir", "/tmp/strata_output"))

        inputs = deserialize_inputs(manifest)
        outputs, stdout_text, stderr_text, mutation_warnings = execute_cell(source, inputs)

        serialized: dict[str, Any] = {}
        for var_name, value in outputs.items():
            try:
                serialized[var_name] = _ser.serialize_value(value, output_dir, var_name)
            except Exception as e:
                serialized[var_name] = {"error": str(e), "type": type(value).__name__}

        result = {
            "success": True,
            "variables": serialized,
            "stdout": stdout_text,
            "stderr": stderr_text,
            "mutation_warnings": mutation_warnings,
        }

    except Exception as e:
        result = {
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc(),
            "variables": {},
            "stdout": stdout_text,
            "stderr": stderr_text,
            "mutation_warnings": [],
        }
        output_dir = Path(manifest.get("output_dir", "/tmp/strata_output"))
        sys.exit(1)

    finally:
        result_path = Path(manifest.get("output_dir", "/tmp/strata_output")) / "manifest.json"
        result_path.parent.mkdir(parents=True, exist_ok=True)
        with open(result_path, "w") as f:
            json.dump(result, f, indent=2)


if __name__ == "__main__":
    main()
