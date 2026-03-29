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


def _load_local_module(filename: str, module_name: str):
    """Load a sibling module by absolute file path."""
    module_path = Path(__file__).parent / filename
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


_ser = _load_local_module("serializer.py", "_nb_serializer")
_immut = _load_local_module("immutability.py", "_nb_immutability")


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


def _serialize_mutation_warning(warning: Any) -> dict[str, Any]:
    """Convert mutation warnings to JSON-safe dicts."""
    if isinstance(warning, dict):
        return warning
    return {
        "var_name": getattr(warning, "var_name", ""),
        "message": getattr(warning, "message", ""),
        "suggestion": getattr(warning, "suggestion", None),
    }


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
        input_snapshots = _immut.snapshot_inputs(namespace, list(namespace_before))

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

        mutation_warnings = [
            _serialize_mutation_warning(warning)
            for warning in _immut.detect_mutations(namespace, input_snapshots)
        ]
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
