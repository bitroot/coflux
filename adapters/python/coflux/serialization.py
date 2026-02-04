"""Serialization utilities for the executor."""

from __future__ import annotations

import importlib
import json
import os
import pickle
import tempfile
from pathlib import Path
from typing import Any

# Try to import pydantic
try:
    import pydantic
    HAS_PYDANTIC = True
except ImportError:
    pydantic = None  # type: ignore
    HAS_PYDANTIC = False


def deserialize_argument(arg: dict[str, Any]) -> Any:
    """Deserialize an argument from the protocol format.

    Args:
        arg: Argument dict with type, format, and value/path.

    Returns:
        Deserialized Python value.
    """
    arg_type = arg.get("type")
    format_name = arg.get("format", "json")
    metadata = arg.get("metadata", {})

    if arg_type == "inline":
        value = arg.get("value")
        return _deserialize_value(value, format_name, metadata)
    elif arg_type == "file":
        path = arg.get("path")
        if not path:
            raise ValueError("File argument missing path")
        return _deserialize_file(path, format_name, metadata)
    else:
        raise ValueError(f"Unknown argument type: {arg_type}")


def _deserialize_value(value: Any, format_name: str, metadata: dict[str, Any] | None = None) -> Any:
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


def _deserialize_file(path: str, format_name: str, metadata: dict[str, Any] | None = None) -> Any:
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
        raise ValueError("Pydantic format requires model_class and model_module in metadata")

    # Import the module and get the model class
    try:
        module = importlib.import_module(model_module)
        model_class = getattr(module, model_class_name)
    except (ImportError, AttributeError) as e:
        raise ValueError(f"Failed to load Pydantic model {model_module}.{model_class_name}: {e}")

    # Validate it's a Pydantic model
    if not (hasattr(model_class, "model_validate") or hasattr(model_class, "parse_obj")):
        raise ValueError(f"{model_class_name} is not a Pydantic model")

    # Deserialize using Pydantic v2 API (model_validate) or v1 API (parse_obj)
    if hasattr(model_class, "model_validate"):
        return model_class.model_validate(value)
    else:
        return model_class.parse_obj(value)


def _is_pydantic_model(value: Any) -> bool:
    """Check if a value is a Pydantic BaseModel instance."""
    if not HAS_PYDANTIC:
        return False
    return isinstance(value, pydantic.BaseModel)


def serialize_result(value: Any) -> dict[str, Any] | None:
    """Serialize a result value to the protocol format.

    Args:
        value: The Python value to serialize.

    Returns:
        Serialized value dict or None if value is None.
    """
    if value is None:
        return None

    # Try Pydantic model first (before JSON, since Pydantic models are dict-like)
    if _is_pydantic_model(value):
        # Serialize using Pydantic v2 API (model_dump) or v1 API (dict)
        if hasattr(value, "model_dump"):
            json_value = value.model_dump(mode="json")
        else:
            json_value = value.dict()
        return {
            "type": "inline",
            "format": "pydantic",
            "value": json_value,
            "metadata": {
                "model_class": value.__class__.__name__,
                "model_module": value.__class__.__module__,
            },
        }

    # Try JSON for simple types
    if _is_json_serializable(value):
        return {
            "type": "inline",
            "format": "json",
            "value": value,
        }

    # Try pandas DataFrame
    if _is_dataframe(value):
        path = _serialize_to_temp_file(value, "parquet")
        return {
            "type": "file",
            "format": "parquet",
            "path": path,
        }

    # Fall back to pickle
    path = _serialize_to_temp_file(value, "pickle")
    return {
        "type": "file",
        "format": "pickle",
        "path": path,
    }


def _is_json_serializable(value: Any) -> bool:
    """Check if a value can be serialized to JSON."""
    if value is None:
        return True
    if isinstance(value, (bool, int, float, str)):
        return True
    if isinstance(value, (list, tuple)):
        return all(_is_json_serializable(v) for v in value)
    if isinstance(value, dict):
        return all(
            isinstance(k, str) and _is_json_serializable(v)
            for k, v in value.items()
        )
    return False


def _is_dataframe(value: Any) -> bool:
    """Check if a value is a pandas DataFrame."""
    try:
        import pandas as pd
        return isinstance(value, pd.DataFrame)
    except ImportError:
        return False


def _serialize_to_temp_file(value: Any, format_name: str) -> str:
    """Serialize a value to a temporary file.

    Returns:
        Path to the temporary file.
    """
    if format_name == "pickle":
        fd, path = tempfile.mkstemp(suffix=".pickle")
        with open(fd, "wb") as f:
            pickle.dump(value, f)
        return path
    elif format_name == "parquet":
        import pandas as pd
        fd, path = tempfile.mkstemp(suffix=".parquet")
        os.close(fd)  # Close fd since to_parquet opens by path
        if isinstance(value, pd.DataFrame):
            value.to_parquet(path)
        return path
    elif format_name == "json":
        fd, path = tempfile.mkstemp(suffix=".json")
        with open(fd, "w") as f:
            json.dump(value, f)
        return path
    else:
        raise ValueError(f"Unknown format: {format_name}")
