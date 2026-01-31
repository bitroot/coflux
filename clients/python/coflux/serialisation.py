import abc
import collections
import importlib
import io
import json
import pickle
import typing as t

import pydantic

try:
    import pandas
except ImportError:
    pandas = None

from . import blobs, config, models, types

T = t.TypeVar("T")


def _json_dumps(obj: t.Any) -> str:
    return json.dumps(obj, separators=(",", ":"))


class Serialiser(abc.ABC):
    @abc.abstractmethod
    def serialise(
        self, value: t.Any
    ) -> tuple[str, io.BytesIO, dict[str, t.Any]] | None:
        raise NotImplementedError

    @abc.abstractmethod
    def deserialise(
        self, format: str, buffer: io.BytesIO, metadata: dict[str, t.Any]
    ) -> t.Any | None:
        raise NotImplementedError


class PickleSerialiser(Serialiser):
    def serialise(
        self, value: t.Any
    ) -> tuple[str, io.BytesIO, dict[str, t.Any]] | None:
        try:
            buffer = io.BytesIO()
            pickle.dump(value, buffer)
            return "pickle", buffer, {"type": str(type(value))}
        except pickle.PicklingError:
            return None

    def deserialise(
        self, format: str, buffer: io.BytesIO, metadata: dict[str, t.Any]
    ) -> t.Any | None:
        if format != "pickle":
            return None
        return pickle.loads(buffer.getbuffer())


class PydanticSerialiser(Serialiser):
    def serialise(
        self, value: t.Any
    ) -> tuple[str, io.BytesIO, dict[str, t.Any]] | None:
        if not isinstance(value, pydantic.BaseModel):
            return None
        buffer = io.BytesIO()
        json_data = value.model_dump_json().encode()
        buffer.write(json_data)
        model_class = value.__class__
        model = f"{model_class.__module__}.{model_class.__name__}"
        return "json", buffer, {"model": model}

    def deserialise(
        self, format: str, buffer: io.BytesIO, metadata: dict[str, t.Any]
    ) -> t.Any | None:
        if format != "json" or "model" not in metadata:
            return None
        json_data = buffer.read().decode()
        model = metadata["model"]
        module_name, class_name = model.rsplit(".", 1)
        # TODO: support configuring which modules can be loaded?
        module = importlib.import_module(module_name)
        model_class = getattr(module, class_name)
        if not model_class:
            raise ValueError(f"Model '{model}' not found")
        if not issubclass(model_class, pydantic.BaseModel):
            raise ValueError(f"Model '{model}' isn't a subclass of BaseModel")
        return model_class.model_validate_json(json_data)


class PandasSerialiser(Serialiser):
    def serialise(
        self, value: t.Any
    ) -> tuple[str, io.BytesIO, dict[str, t.Any]] | None:
        # TODO: support series
        if pandas and isinstance(value, pandas.DataFrame):
            try:
                buffer = io.BytesIO()
                value.to_parquet(buffer)
                return "parquet", buffer, {"shape": value.shape}
            except ImportError:
                return None

    def deserialise(
        self, format: str, buffer: io.BytesIO, metadata: dict[str, t.Any]
    ) -> t.Any | None:
        if format != "parquet":
            return None
        if not pandas:
            raise Exception("Pandas dependency not available")
        # TODO: check metadata["content_type"]
        return pandas.read_parquet(buffer)


def _create(config_: config.SerialiserConfig):
    if config_.type == "pickle":
        return PickleSerialiser()
    elif config_.type == "pydantic":
        return PydanticSerialiser()
    elif config_.type == "pandas":
        return PandasSerialiser()
    else:
        raise ValueError("unrecognised serialiser config")


class Manager:
    def __init__(
        self,
        serialiser_configs: list[config.SerialiserConfig],
        blob_threshold: int,
        blob_manager: blobs.Manager,
    ):
        self._serialisers = [_create(c) for c in serialiser_configs]
        self._blob_threshold = blob_threshold
        self._blob_manager = blob_manager

    def serialise(self, value: t.Any) -> types.Value:
        references: list[types.Reference] = []

        def _serialise(value: t.Any) -> t.Any:
            if value is None or isinstance(value, (str, bool, int, float)):
                return value
            elif isinstance(value, list):
                return [_serialise(v) for v in value]
            elif isinstance(value, dict):
                items = (
                    value.items()
                    if isinstance(value, collections.OrderedDict)
                    else sorted(value.items(), key=lambda kv: repr(kv[0]))
                )
                return {
                    "type": "dict",
                    "items": [_serialise(x) for kv in items for x in kv],
                }
            elif isinstance(value, set):
                return {
                    "type": "set",
                    "items": [_serialise(x) for x in sorted(value)],
                }
            elif isinstance(value, models.Execution):
                # TODO: better handle id being none
                assert value.id is not None
                # Include metadata in reference for log storage
                references.append(("execution", value.id, value.metadata))
                return {"type": "ref", "index": len(references) - 1}
            elif isinstance(value, models.Asset):
                # Include metadata in reference for log storage
                references.append(("asset", value.id, value.metadata))
                return {"type": "ref", "index": len(references) - 1}
            elif isinstance(value, tuple):
                # TODO: include name
                return {"type": "tuple", "items": [_serialise(x) for x in value]}
            else:
                for serialiser in self._serialisers:
                    result = serialiser.serialise(value)
                    if result is not None:
                        format, buffer, metadata = result
                        blob_key = self._blob_manager.put(buffer)
                        size = buffer.getbuffer().nbytes
                        references.append(
                            ("fragment", format, blob_key, size, metadata)
                        )
                        return {"type": "ref", "index": len(references) - 1}
                raise Exception(f"no serialiser for type '{type(value)}'")

        data = _serialise(value)
        encoded_data = _json_dumps(data).encode()
        size = len(encoded_data)
        if size > self._blob_threshold:
            buffer = io.BytesIO(encoded_data)
            blob_key = self._blob_manager.put(buffer)
            return ("blob", blob_key, size, references)
        else:
            return ("raw", data, references)

    def _get_value_data(
        self, value: types.Value
    ) -> tuple[t.Any, list[types.Reference]]:
        match value:
            case ("blob", key, _, references):
                result = self._blob_manager.get(key)
                return json.load(result), references
            case ("raw", data, references):
                return data, references

    def deserialise(self, value: types.Value) -> t.Any:
        data, references = self._get_value_data(value)

        def _deserialise(data: t.Any):
            if data is None or isinstance(data, (str, bool, int, float)):
                return data
            elif isinstance(data, list):
                return [_deserialise(v) for v in data]
            elif isinstance(data, dict):
                match data["type"]:
                    case "dict":
                        pairs = zip(data["items"][::2], data["items"][1::2])
                        return {_deserialise(k): _deserialise(v) for k, v in pairs}
                    case "set":
                        return {_deserialise(x) for x in data["items"]}
                    case "tuple":
                        return tuple(_deserialise(x) for x in data["items"])
                    case "ref":
                        reference = references[data["index"]]
                        match reference:
                            case ("execution", execution_id, exec_metadata):
                                return models.Execution(execution_id, exec_metadata)
                            case ("asset", asset_id, asset_metadata):
                                return models.Asset(asset_id, asset_metadata)
                            case ("fragment", format, blob_key, _size, metadata):
                                data = self._blob_manager.get(blob_key)
                                for serialiser in self._serialisers:
                                    result = serialiser.deserialise(
                                        format, data, metadata
                                    )
                                    if result is not None:
                                        return result
                                raise Exception(
                                    f"Couldn't deserialise fragment ({format})"
                                )
                            case other:
                                raise ValueError(f"Unknown reference type: {other}")
                    case other:
                        raise Exception(f"unhandled data type ({other})")
            else:
                raise Exception(f"unhandled data type ({type(data)})")

        return _deserialise(data)
