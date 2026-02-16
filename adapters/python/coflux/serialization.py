"""Serialization utilities for the executor."""

from __future__ import annotations

import collections
import importlib
import io
import json
import pickle
import tempfile
from pathlib import Path
from typing import Any, Callable

from .models import Asset, AssetMetadata, Execution

# Try to import pydantic
try:
    import pydantic

    HAS_PYDANTIC = True
except ImportError:
    pydantic = None  # type: ignore
    HAS_PYDANTIC = False

# Values larger than this are passed via temp files rather than inline
# in the stdio JSON protocol between the Python adapter and Go CLI.
TRANSFER_THRESHOLD = 64 * 1024  # 64KB


def _write_temp_file(data: bytes) -> str:
    """Write data to a temp file and return the path."""
    fd, path = tempfile.mkstemp(prefix="coflux_")
    with open(fd, "wb") as f:
        f.write(data)
    return path


def encode_value(
    value: Any,
    write_temp_file: Callable[[bytes], str] = _write_temp_file,
) -> tuple[Any, list[list[Any]]]:
    """Encode a Python value using the custom JSON value format.

    Converts Python types to a JSON-serializable structure using typed wrappers
    for dicts, sets, and tuples. Unsupported types are serialized as pickle
    fragment references.

    Args:
        value: The Python value to encode.
        write_temp_file: Callable that writes bytes to a temp file and returns
            the path. Used for pickle fragment references.

    Returns:
        Tuple of (data, references) where data is JSON-serializable and
        references is a list of reference arrays.
    """
    references: list[list[Any]] = []

    def _encode(v: Any) -> Any:
        if v is None or isinstance(v, (str, bool, int, float)):
            return v
        elif isinstance(v, list):
            return [_encode(x) for x in v]
        elif isinstance(v, dict):
            items = (
                v.items()
                if isinstance(v, collections.OrderedDict)
                else sorted(v.items(), key=lambda kv: repr(kv[0]))
            )
            return {
                "type": "dict",
                "items": [_encode(x) for kv in items for x in kv],
            }
        elif isinstance(v, set):
            return {
                "type": "set",
                "items": [_encode(x) for x in sorted(v, key=repr)],
            }
        elif isinstance(v, tuple):
            return {"type": "tuple", "items": [_encode(x) for x in v]}
        elif isinstance(v, Execution):
            references.append(["execution", v.id, v.module, v.target])
            return {"type": "ref", "index": len(references) - 1}
        elif isinstance(v, Asset):
            m = v.metadata
            references.append(["asset", v.id, m.name if m else None, m.total_count if m else None, m.total_size if m else None])
            return {"type": "ref", "index": len(references) - 1}
        elif HAS_PYDANTIC and isinstance(v, pydantic.BaseModel):
            model_class = v.__class__
            model_fqn = f"{model_class.__module__}.{model_class.__name__}"
            json_data = v.model_dump_json().encode()
            path = write_temp_file(json_data)
            references.append([
                "fragment",
                "json",
                path,
                len(json_data),
                {"model": model_fqn},
            ])
            return {"type": "ref", "index": len(references) - 1}
        else:
            # Serialize with pickle and write to temp file as fragment
            try:
                buffer = io.BytesIO()
                pickle.dump(v, buffer)
                data = buffer.getvalue()
                path = write_temp_file(data)
                references.append([
                    "fragment",
                    "pickle",
                    path,  # file path; CLI uploads and replaces with blob key
                    len(data),
                    {"type": str(type(v).__name__)},
                ])
                return {"type": "ref", "index": len(references) - 1}
            except Exception:
                return repr(v)

    data = _encode(value)
    return data, references


def serialize_result(value: Any) -> dict[str, Any]:
    """Serialize a result value to the protocol format.

    Uses the custom JSON value encoding (dict/set/tuple types, fragment refs
    for unsupported types). The result is always JSON-format data.

    Args:
        value: The Python value to serialize.

    Returns:
        Serialized value dict.
    """
    data, references = encode_value(value)
    encoded = json.dumps(data, separators=(",", ":")).encode()

    if len(encoded) > TRANSFER_THRESHOLD:
        path = _write_temp_file(encoded)
        return {
            "type": "file",
            "format": "json",
            "path": path,
            "references": references,
        }
    else:
        return {
            "type": "inline",
            "format": "json",
            "value": data,
            "references": references,
        }


def decode_value(data: Any, references: list[list[Any]] | None = None) -> Any:
    """Decode custom JSON format back to Python types.

    Reverses the encoding done by encode_value.

    Args:
        data: The encoded data (output of encode_value / JSON.parse of blob).
        references: The references array accompanying the value.

    Returns:
        Native Python value.
    """
    refs = references or []

    def _decode(v: Any) -> Any:
        if v is None or isinstance(v, (str, bool, int, float)):
            return v
        elif isinstance(v, list):
            return [_decode(x) for x in v]
        elif isinstance(v, dict) and "type" in v:
            t = v["type"]
            if t == "dict":
                items = v["items"]
                return {
                    _decode(items[i]): _decode(items[i + 1])
                    for i in range(0, len(items), 2)
                }
            elif t == "set":
                return {_decode(x) for x in v["items"]}
            elif t == "tuple":
                return tuple(_decode(x) for x in v["items"])
            elif t == "ref":
                return _resolve_ref(v["index"])
            else:
                return v
        else:
            return v

    def _resolve_ref(index: int) -> Any:
        if index >= len(refs):
            return None
        ref = refs[index]
        if not isinstance(ref, list) or len(ref) < 2:
            return None
        ref_type = ref[0]
        if ref_type == "fragment":
            # ["fragment", format, path, size, metadata]
            fmt = ref[1] if len(ref) > 1 else "pickle"
            path = ref[2] if len(ref) > 2 else None
            metadata = ref[4] if len(ref) > 4 and isinstance(ref[4], dict) else {}
            if not path:
                return None
            if fmt == "pickle":
                with open(path, "rb") as f:
                    return pickle.load(f)
            elif fmt == "json" and "model" in metadata:
                model_fqn = metadata["model"]
                module_name, class_name = model_fqn.rsplit(".", 1)
                module = importlib.import_module(module_name)
                model_class = getattr(module, class_name)
                with open(path, "rb") as f:
                    return model_class.model_validate_json(f.read())
            elif fmt == "json":
                with open(path, "r") as f:
                    return json.load(f)
            return None
        elif ref_type == "execution":
            return Execution(ref[1], ref[2], ref[3])
        elif ref_type == "asset":
            asset_id = ref[1]
            metadata = None
            if len(ref) >= 5:
                metadata = AssetMetadata(
                    name=ref[2],
                    total_count=ref[3],
                    total_size=ref[4],
                )
            return Asset(asset_id, metadata)
        return None

    return _decode(data)


def deserialize_argument(arg: dict[str, Any]) -> Any:
    """Deserialize an argument from the protocol format.

    Args:
        arg: Argument dict with type, format, and value/path.

    Returns:
        Deserialized Python value.
    """
    references = arg.get("references", [])

    arg_type = arg.get("type")
    format_name = arg.get("format", "json")
    metadata = arg.get("metadata", {})

    if arg_type == "inline":
        value = arg.get("value")
        raw = _deserialize_value(value, format_name, metadata)
        if format_name == "json":
            return decode_value(raw, references)
        return raw
    elif arg_type == "file":
        path = arg.get("path")
        if not path:
            raise ValueError("File argument missing path")
        raw = _deserialize_file(path, format_name, metadata)
        if format_name == "json":
            return decode_value(raw, references)
        return raw
    else:
        raise ValueError(f"Unknown argument type: {arg_type}")


def _deserialize_value(
    value: Any, format_name: str, metadata: dict[str, Any] | None = None
) -> Any:
    """Deserialize an inline value."""
    if format_name == "json":
        # Value is already deserialized from JSON
        return value
    elif format_name == "pydantic":
        # Pydantic model - value is JSON, metadata contains model class
        return _deserialize_pydantic(value, metadata or {})
    elif format_name == "pickle":
        # Inline pickle is base64-encoded
        import base64

        data = base64.b64decode(value)
        return pickle.loads(data)
    else:
        raise ValueError(f"Unknown format for inline value: {format_name}")


def _deserialize_file(
    path: str, format_name: str, metadata: dict[str, Any] | None = None
) -> Any:
    """Deserialize a value from a file."""
    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"Argument file not found: {path}")

    if format_name == "json":
        with open(file_path, "r") as f:
            return json.load(f)
    elif format_name == "pickle":
        with open(file_path, "rb") as f:
            return pickle.load(f)
    elif format_name == "parquet":
        try:
            import pandas as pd

            return pd.read_parquet(file_path)
        except ImportError:
            raise ImportError("pandas is required to deserialize parquet format")
    elif format_name == "pydantic":
        with open(file_path, "r") as f:
            value = json.load(f)
        return _deserialize_pydantic(value, metadata or {})
    else:
        raise ValueError(f"Unknown format: {format_name}")


def _deserialize_pydantic(value: Any, metadata: dict[str, Any]) -> Any:
    """Deserialize a Pydantic model from JSON value and metadata."""
    if not HAS_PYDANTIC:
        raise ImportError("pydantic is required to deserialize pydantic format")

    model_class_name = metadata.get("model_class")
    model_module = metadata.get("model_module")

    if not model_class_name or not model_module:
        raise ValueError(
            "Pydantic format requires model_class and model_module in metadata"
        )

    # Import the module and get the model class
    try:
        module = importlib.import_module(model_module)
        model_class = getattr(module, model_class_name)
    except (ImportError, AttributeError) as e:
        raise ValueError(
            f"Failed to load Pydantic model {model_module}.{model_class_name}: {e}"
        )

    # Validate it's a Pydantic model
    if not (
        hasattr(model_class, "model_validate") or hasattr(model_class, "parse_obj")
    ):
        raise ValueError(f"{model_class_name} is not a Pydantic model")

    # Deserialize using Pydantic v2 API (model_validate) or v1 API (parse_obj)
    if hasattr(model_class, "model_validate"):
        return model_class.model_validate(value)
    else:
        return model_class.parse_obj(value)
