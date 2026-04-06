"""Shared serialization/deserialization for notebook cell values.

Supports six content types:
  arrow/ipc    — PyArrow Tables, pandas DataFrames/Series, numpy arrays
  json/object  — dicts, lists, scalars (int/float/str/bool/None)
  image/png    — Displayable PNG output (figures, images)
  module/import — Python module objects (re-imported by name on read)
  module/cell  — Synthetic module export for top-level defs/classes
  module/cell-instance — Instance of a synthetic notebook-exported class
  pickle/object — everything else

This module is loaded dynamically by harness.py, pool_worker.py, and
inspect_repl.py via ``importlib.util``, since those scripts run inside
the notebook's own venv and cannot ``import strata``.

Loading pattern (used in each subprocess script):

    import importlib.util as _ilu
    from pathlib import Path as _Path

    def _load_serializer():
        _p = _Path(__file__).parent / "serializer.py"
        _spec = _ilu.spec_from_file_location("_nb_serializer", _p)
        _m = _ilu.module_from_spec(_spec)
        _spec.loader.exec_module(_m)
        return _m

    _ser = _load_serializer()
"""

from __future__ import annotations

import base64
import io
import json
import os
import pickle
import sys
from pathlib import Path
from typing import Any, Protocol

OBJECT_CODEC_ENV_VAR = "STRATA_NOTEBOOK_OBJECT_CODEC"
_CODEC_ENVELOPE_TAG = "strata.notebook.object_codec.v1"
_CELL_INSTANCE_STATE_TAG = "strata.notebook.cell_instance_state.v1"


class ObjectCodec(Protocol):
    """Pluggable object serializer backend for notebook runtime values."""

    name: str

    def dumps(self, value: Any) -> bytes:
        """Serialize *value* to backend-specific bytes."""

    def loads(self, data: bytes) -> Any:
        """Deserialize backend-specific bytes to a Python object."""


class _PickleObjectCodec:
    name = "pickle"

    def dumps(self, value: Any) -> bytes:
        return pickle.dumps(value, protocol=5)

    def loads(self, data: bytes) -> Any:
        return pickle.loads(data)


class _CloudPickleObjectCodec:
    name = "cloudpickle"

    def __init__(self) -> None:
        try:
            import cloudpickle  # type: ignore[import-not-found]
        except ImportError as exc:  # pragma: no cover - optional backend
            raise ValueError(
                "Object codec 'cloudpickle' requires the 'cloudpickle' package to be installed"
            ) from exc
        self._cloudpickle = cloudpickle

    def dumps(self, value: Any) -> bytes:
        return self._cloudpickle.dumps(value, protocol=5)

    def loads(self, data: bytes) -> Any:
        return pickle.loads(data)


def _resolve_object_codec(codec_name: str | None = None) -> ObjectCodec:
    """Return the configured object codec implementation."""
    selected = (codec_name or os.environ.get(OBJECT_CODEC_ENV_VAR, "pickle")).strip().lower()
    if selected == "pickle":
        return _PickleObjectCodec()
    if selected == "cloudpickle":
        return _CloudPickleObjectCodec()
    raise ValueError(
        f"Unknown notebook object codec '{selected}'. "
        "Supported codecs: pickle, cloudpickle"
    )


def _wrap_codec_payload(codec_name: str, payload: bytes) -> dict[str, Any]:
    return {
        "__strata_object_codec__": _CODEC_ENVELOPE_TAG,
        "codec": codec_name,
        "payload": payload,
    }


def _unwrap_codec_payload(obj: Any) -> tuple[str, bytes] | None:
    if not isinstance(obj, dict):
        return None
    if obj.get("__strata_object_codec__") != _CODEC_ENVELOPE_TAG:
        return None
    codec_name = obj.get("codec")
    payload = obj.get("payload")
    if not isinstance(codec_name, str) or not isinstance(payload, bytes):
        raise ValueError("Invalid notebook object codec envelope")
    return codec_name, payload

# ---------------------------------------------------------------------------
# Content-type detection
# ---------------------------------------------------------------------------


def detect_content_type(value: Any, variable_name: str | None = None) -> str:
    """Return the content type string for *value*.

    Probes pyarrow, pandas, and numpy with separate try/except blocks so
    that the absence of one package does not mask another.
    """
    import types

    try:
        import pyarrow as pa
        if isinstance(value, (pa.Table, pa.RecordBatch)):
            return "arrow/ipc"
    except ImportError:
        pass

    try:
        import pandas as pd
        if isinstance(value, (pd.DataFrame, pd.Series)):
            return "arrow/ipc"
    except ImportError:
        pass

    try:
        import numpy as np
        if isinstance(value, np.ndarray):
            return "arrow/ipc"
    except ImportError:
        pass

    if variable_name == "_" and _is_png_display_value(value):
        return "image/png"

    if isinstance(value, (dict, list, int, float, str, bool, type(None))):
        try:
            json.dumps(value)
            return "json/object"
        except (TypeError, ValueError):
            pass

    if isinstance(value, types.ModuleType):
        return "module/import"

    if _is_cell_module_instance(value):
        return "module/cell-instance"

    return "pickle/object"


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------


def serialize_value(
    value: Any, output_dir: Path | str, variable_name: str
) -> dict[str, Any]:
    """Serialize *value* to *output_dir* and return a metadata dict.

    The metadata dict always contains:
      content_type  — one of the four content types above
      file          — filename written (relative to output_dir)
      bytes         — file size in bytes
      preview       — a JSON-safe preview of the value
    Arrow results additionally include ``rows`` and ``columns``.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    content_type = detect_content_type(value, variable_name)

    if content_type == "arrow/ipc":
        try:
            return _serialize_arrow(value, output_dir, variable_name)
        except (ImportError, ValueError):
            # pyarrow unavailable — fall back to JSON with table metadata
            return _serialize_dataframe_json(value, output_dir, variable_name)
    elif content_type == "image/png":
        return _serialize_image_png(value, output_dir, variable_name)
    elif content_type == "json/object":
        return _serialize_json(value, output_dir, variable_name)
    elif content_type == "module/import":
        return _serialize_module(value, output_dir, variable_name)
    elif content_type == "module/cell-instance":
        return _serialize_cell_instance(value, output_dir, variable_name)
    else:
        return _serialize_pickle(value, output_dir, variable_name)


def _serialize_arrow(
    value: Any, output_dir: Path, variable_name: str
) -> dict[str, Any]:
    import pyarrow as pa

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
                    raise ValueError(f"Cannot convert {type(value)} to Arrow")
        except ImportError:
            raise ValueError("pandas/numpy not available for Arrow conversion")

    filename = f"{variable_name}.arrow"
    filepath = output_dir / filename

    with open(filepath, "wb") as f:
        writer = pa.ipc.new_stream(f, table.schema)
        writer.write_table(table)
        writer.close()

    preview = []
    for i in range(min(20, table.num_rows)):
        preview.append([col[i].as_py() for col in table.columns])

    return {
        "content_type": "arrow/ipc",
        "file": filename,
        "rows": table.num_rows,
        "columns": table.column_names,
        "bytes": filepath.stat().st_size,
        "preview": preview,
    }


def _is_png_display_value(value: Any) -> bool:
    repr_png = getattr(value, "_repr_png_", None)
    if callable(repr_png):
        return True

    try:
        from matplotlib.figure import Figure

        if isinstance(value, Figure):
            return True
    except ImportError:
        pass

    try:
        from PIL import Image as _PILImage

        if isinstance(value, _PILImage.Image):
            return True
    except ImportError:
        pass

    return False


def _serialize_image_png(
    value: Any, output_dir: Path, variable_name: str
) -> dict[str, Any]:
    png_bytes: bytes | None = None
    width: int | None = None
    height: int | None = None

    repr_png = getattr(value, "_repr_png_", None)
    if callable(repr_png):
        raw = repr_png()
        if isinstance(raw, str):
            png_bytes = raw.encode("latin1")
        elif isinstance(raw, (bytes, bytearray, memoryview)):
            png_bytes = bytes(raw)
        elif raw is not None:
            raise ValueError("_repr_png_() must return bytes-like data")

    if png_bytes is None:
        try:
            from matplotlib.figure import Figure

            if isinstance(value, Figure):
                buffer = io.BytesIO()
                value.savefig(buffer, format="png")
                png_bytes = buffer.getvalue()
                width = int(round(value.get_figwidth() * value.dpi))
                height = int(round(value.get_figheight() * value.dpi))
        except ImportError:
            pass

    if png_bytes is None:
        try:
            from PIL import Image as _PILImage

            if isinstance(value, _PILImage.Image):
                buffer = io.BytesIO()
                value.save(buffer, format="PNG")
                png_bytes = buffer.getvalue()
                width, height = value.size
        except ImportError:
            pass

    if png_bytes is None:
        raise ValueError(f"Cannot serialize {type(value)} as image/png")

    if width is None or height is None:
        try:
            from PIL import Image as _PILImage

            with _PILImage.open(io.BytesIO(png_bytes)) as image:
                width, height = image.size
        except Exception:
            width = None
            height = None

    filename = f"{variable_name}.png"
    filepath = output_dir / filename
    with open(filepath, "wb") as f:
        f.write(png_bytes)

    return {
        "content_type": "image/png",
        "file": filename,
        "bytes": filepath.stat().st_size,
        "inline_data_url": (
            f"data:image/png;base64,{base64.b64encode(png_bytes).decode('ascii')}"
        ),
        "width": width,
        "height": height,
        "preview": None,
    }


def _serialize_dataframe_json(
    value: Any, output_dir: Path, variable_name: str
) -> dict[str, Any]:
    """JSON fallback for DataFrames when pyarrow is unavailable.

    Produces the same metadata shape as ``_serialize_arrow`` so the
    frontend renders it as a table.
    """
    try:
        import pandas as pd
        if isinstance(value, pd.Series):
            value = value.to_frame()
        columns = list(value.columns)
        num_rows = len(value)
        preview = value.head(20).values.tolist()
        payload = value.to_dict(orient="list")
    except Exception:
        columns = []
        num_rows = 0
        preview = []
        payload = {}

    filename = f"{variable_name}.json"
    filepath = output_dir / filename
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(payload, f)

    return {
        "content_type": "arrow/ipc",
        "file": filename,
        "rows": num_rows,
        "columns": columns,
        "bytes": filepath.stat().st_size,
        "preview": preview,
    }


def _serialize_json(
    value: Any, output_dir: Path, variable_name: str
) -> dict[str, Any]:
    filename = f"{variable_name}.json"
    filepath = output_dir / filename
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(value, f, indent=2)
    return {
        "content_type": "json/object",
        "file": filename,
        "bytes": filepath.stat().st_size,
        "preview": value,
    }


def _serialize_module(
    value: Any, output_dir: Path, variable_name: str
) -> dict[str, Any]:
    module_name = getattr(value, "__name__", variable_name)
    filename = f"{variable_name}.module.json"
    filepath = output_dir / filename
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump({"module_name": module_name}, f)
    return {
        "content_type": "module/import",
        "file": filename,
        "bytes": filepath.stat().st_size,
        "preview": f"<module '{module_name}'>",
    }


def _serialize_cell_instance(
    value: Any, output_dir: Path, variable_name: str
) -> dict[str, Any]:
    module = sys.modules.get(type(value).__module__)
    module_source = getattr(module, "__strata_cell_module_source__", None)
    if not isinstance(module_source, str) or not module_source:
        raise ValueError(
            f"Cannot serialize notebook-exported instance '{variable_name}' "
            "because its synthetic module source is unavailable"
        )

    state = _extract_cell_instance_state(value)
    codec = _resolve_object_codec()
    state_bytes = codec.dumps(state)
    filename = f"{variable_name}.cell_instance.pickle"
    filepath = output_dir / filename
    payload = {
        "module_name": type(value).__module__,
        "class_name": type(value).__name__,
        "source": module_source,
        "state_codec": codec.name,
        "state_payload": state_bytes,
    }
    with open(filepath, "wb") as f:
        pickle.dump(payload, f, protocol=5)

    type_name = type(value).__name__
    return {
        "content_type": "module/cell-instance",
        "file": filename,
        "bytes": filepath.stat().st_size,
        "codec": codec.name,
        "type": type_name,
        "preview": f"<{type_name} object>",
    }


def _serialize_pickle(
    value: Any, output_dir: Path, variable_name: str
) -> dict[str, Any]:
    filename = f"{variable_name}.pickle"
    filepath = output_dir / filename
    type_name = type(value).__name__
    try:
        codec = _resolve_object_codec()
        payload = codec.dumps(value)
        envelope = _wrap_codec_payload(codec.name, payload)
        with open(filepath, "wb") as f:
            pickle.dump(envelope, f, protocol=5)
    except Exception as e:
        return {
            "content_type": "pickle/object",
            "file": None,
            "bytes": 0,
            "type": type_name,
            "preview": f"<{type_name} object>",
            "error": f"Failed to pickle: {e}",
        }
    return {
        "content_type": "pickle/object",
        "file": filename,
        "bytes": filepath.stat().st_size,
        "codec": codec.name,
        "type": type_name,
        "preview": f"<{type_name} object>",
    }


# ---------------------------------------------------------------------------
# Deserialization
# ---------------------------------------------------------------------------

# Extension → content-type mapping (also used by executor._store_outputs)
EXT_TO_CONTENT_TYPE: dict[str, str] = {
    ".arrow": "arrow/ipc",
    ".json": "json/object",
    ".pickle": "pickle/object",
    ".module.json": "module/import",
    ".cell_module.json": "module/cell",
    ".cell_instance.pickle": "module/cell-instance",
}


def deserialize_value(
    content_type: str, file_path: Path | str, output_dir: Path | str | None = None
) -> Any:
    """Deserialize a value from *file_path*.

    *output_dir* is accepted for API compatibility but not required —
    *file_path* is always treated as an absolute (or relative-to-cwd) path.
    """
    file_path = Path(file_path)
    if content_type == "arrow/ipc":
        return _deserialize_arrow(file_path)
    elif content_type == "json/object":
        return _deserialize_json(file_path)
    elif content_type == "pickle/object":
        return _deserialize_pickle(file_path)
    elif content_type == "module/import":
        return _deserialize_module(file_path)
    elif content_type == "module/cell":
        return _deserialize_cell_module(file_path)
    elif content_type == "module/cell-instance":
        return _deserialize_cell_instance(file_path)
    else:
        raise ValueError(f"Unknown content type: {content_type!r}")


def _deserialize_arrow(file_path: Path) -> Any:
    """Read an Arrow IPC stream.

    Returns a pandas DataFrame when pandas is available; falls back to
    a pyarrow Table.  Users expect DataFrames, not raw Arrow.
    """
    import pyarrow as pa

    with open(file_path, "rb") as f:
        reader = pa.ipc.open_stream(f)
        table = reader.read_all()

    try:
        return table.to_pandas()
    except Exception:
        return table


def _deserialize_json(file_path: Path) -> Any:
    with open(file_path, encoding="utf-8") as f:
        return json.load(f)


def _deserialize_pickle(file_path: Path) -> Any:
    with open(file_path, "rb") as f:
        data = pickle.load(f)

    codec_payload = _unwrap_codec_payload(data)
    if codec_payload is None:
        # Backward compatibility: historical notebook artifacts stored raw pickle payloads.
        return data

    codec_name, payload = codec_payload
    codec = _resolve_object_codec(codec_name)
    return codec.loads(payload)


def _deserialize_module(file_path: Path) -> Any:
    import importlib

    with open(file_path, encoding="utf-8") as f:
        data = json.load(f)
    return importlib.import_module(data["module_name"])


def _ensure_cell_module(
    module_name: str,
    module_source: str,
    file_path: Path,
):
    import types

    module = sys.modules.get(module_name)
    if module is None:
        module = types.ModuleType(module_name)
        module.__file__ = str(file_path)
        sys.modules[module_name] = module
        exec(compile(module_source, module_name, "exec"), module.__dict__)  # noqa: S102
    module.__dict__["__strata_cell_module_source__"] = module_source
    module.__dict__["__strata_cell_module__"] = True
    for value in module.__dict__.values():
        if isinstance(value, type):
            setattr(value, "__strata_cell_exported_class__", True)
    return module


def _deserialize_cell_module(file_path: Path) -> Any:
    with open(file_path, encoding="utf-8") as f:
        data = json.load(f)

    module_name = data.get("module_name")
    symbol_name = data.get("symbol_name")
    module_source = data.get("source")
    if not isinstance(module_name, str) or not isinstance(symbol_name, str):
        raise ValueError("Invalid exported notebook module descriptor")
    if not isinstance(module_source, str):
        raise ValueError(f"Exported notebook module '{module_name}' has invalid source")

    module = _ensure_cell_module(module_name, module_source, file_path)

    try:
        return getattr(module, symbol_name)
    except AttributeError as exc:
        raise ValueError(
            f"Exported notebook module '{module_name}' does not define '{symbol_name}'"
        ) from exc


def _deserialize_cell_instance(file_path: Path) -> Any:
    with open(file_path, "rb") as f:
        data = pickle.load(f)

    if not isinstance(data, dict):
        raise ValueError("Invalid notebook-exported instance payload")

    module_name = data.get("module_name")
    class_name = data.get("class_name")
    module_source = data.get("source")
    if not isinstance(module_name, str) or not isinstance(class_name, str):
        raise ValueError("Invalid notebook-exported instance descriptor")
    if not isinstance(module_source, str):
        raise ValueError(
            f"Exported notebook instance '{class_name}' has invalid module source"
        )

    module = _ensure_cell_module(module_name, module_source, file_path)
    try:
        cls = getattr(module, class_name)
    except AttributeError as exc:
        raise ValueError(
            f"Exported notebook module '{module_name}' does not define class '{class_name}'"
        ) from exc

    if "state_payload" in data and "state_codec" in data:
        state_codec = data["state_codec"]
        state_payload = data["state_payload"]
        if not isinstance(state_codec, str) or not isinstance(state_payload, bytes):
            raise ValueError("Invalid notebook-exported instance state payload")
        state = _resolve_object_codec(state_codec).loads(state_payload)
    else:
        # Backward compatibility for the first module/cell-instance format.
        state_pickle = data["state_pickle"]
        state = pickle.loads(state_pickle)
    instance = cls.__new__(cls)

    setstate = getattr(instance, "__setstate__", None)
    if callable(setstate):
        setstate(state)
    elif _is_default_cell_instance_state(state):
        _restore_default_cell_instance_state(instance, state)
    elif state is None:
        pass
    elif isinstance(state, dict):
        instance.__dict__.update(state)
    else:
        raise ValueError(
            f"Cannot restore notebook-exported instance '{class_name}' without __setstate__"
        )

    return instance


def _is_cell_module_instance(value: Any) -> bool:
    if isinstance(value, type):
        return False

    module = sys.modules.get(type(value).__module__)
    module_source = getattr(module, "__strata_cell_module_source__", None)
    return bool(
        getattr(type(value), "__strata_cell_exported_class__", False)
        and isinstance(module_source, str)
        and module_source
    )


def _extract_cell_instance_state(value: Any) -> Any:
    getstate = getattr(type(value), "__getstate__", None)
    if callable(getstate) and getstate is not object.__getstate__:
        return value.__getstate__()

    return _extract_default_cell_instance_state(value)


def _extract_default_cell_instance_state(value: Any) -> Any:
    dict_state = dict(value.__dict__) if hasattr(value, "__dict__") else None
    slot_state: dict[str, Any] = {}
    for slot_name in _iter_slot_names(type(value)):
        try:
            slot_state[slot_name] = getattr(value, slot_name)
        except AttributeError:
            continue

    if dict_state is None and not slot_state:
        return None

    return {
        "__strata_cell_instance_state__": _CELL_INSTANCE_STATE_TAG,
        "dict": dict_state,
        "slots": slot_state,
    }


def _is_default_cell_instance_state(state: Any) -> bool:
    return (
        isinstance(state, dict)
        and state.get("__strata_cell_instance_state__") == _CELL_INSTANCE_STATE_TAG
    )


def _restore_default_cell_instance_state(instance: Any, state: Any) -> None:
    if not isinstance(state, dict):
        raise ValueError("Invalid notebook-exported instance state")

    dict_state = state.get("dict")
    slot_state = state.get("slots")

    if dict_state is not None:
        if not isinstance(dict_state, dict):
            raise ValueError("Invalid notebook-exported instance __dict__ state")
        instance.__dict__.update(dict_state)

    if slot_state is not None:
        if not isinstance(slot_state, dict):
            raise ValueError("Invalid notebook-exported instance __slots__ state")
        for slot_name, slot_value in slot_state.items():
            if not isinstance(slot_name, str):
                raise ValueError("Invalid notebook-exported instance slot name")
            setattr(instance, slot_name, slot_value)


def _iter_slot_names(cls: type[Any]) -> list[str]:
    slot_names: list[str] = []
    for klass in cls.__mro__:
        slots = klass.__dict__.get("__slots__")
        if slots is None:
            continue
        if isinstance(slots, str):
            slot_values = [slots]
        else:
            slot_values = list(slots)
        for slot_name in slot_values:
            if slot_name in {"__dict__", "__weakref__"}:
                continue
            if slot_name not in slot_names:
                slot_names.append(slot_name)

    return slot_names
