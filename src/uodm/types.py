import importlib
import importlib.util
import json
import pickle
from enum import Enum
from typing import Any, TypeVar, Union, cast

from motor.core import AgnosticCollection

HAVE_ORJSON = importlib.util.find_spec("orjson") is not None
if HAVE_ORJSON:
    orjson = importlib.import_module("orjson")
else:
    orjson = cast(Any, None)  # Type stub for when orjson is not available

T = TypeVar("T")


class SerializationFormat(str, Enum):
    JSON = "json"
    ORJSON = "orjson"
    PICKLE = "pickle"
    SQLITE = "sqlite"  # Added SQLite as a special format


def get_collection_type():
    from .file_motor import FileMotorCollection
    from .sqlite_motor import SQLiteMotorCollection

    return Union[AgnosticCollection, FileMotorCollection, SQLiteMotorCollection]


CollectionType = Any  # Will be replaced at runtime


def serialize_data(data: Any, format: SerializationFormat) -> bytes:
    if format == SerializationFormat.PICKLE:
        return pickle.dumps(data)
    elif format == SerializationFormat.ORJSON:
        if not HAVE_ORJSON:
            raise ImportError("orjson is required for ORJSON format but it's not installed")
        return orjson.dumps(data)
    else:  # JSON
        return json.dumps(data, ensure_ascii=False, default=str).encode()


def deserialize_data(data: bytes, format: SerializationFormat) -> Any:
    if format == SerializationFormat.PICKLE:
        return pickle.loads(data)
    elif format == SerializationFormat.ORJSON:
        if not HAVE_ORJSON:
            raise ImportError("orjson is required for ORJSON format but it's not installed")
        return orjson.loads(data)
    else:  # JSON
        return json.loads(data)
