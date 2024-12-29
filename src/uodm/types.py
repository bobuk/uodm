import json
import pickle
from enum import Enum
from typing import Any, TypeVar, Union, cast

from motor.core import AgnosticCollection

try:
    import orjson
    HAVE_ORJSON = True
except ImportError:
    HAVE_ORJSON = False
    orjson = cast(Any, None)  # Type stub for when orjson is not available

T = TypeVar("T")

class SerializationFormat(str, Enum):
    JSON = "json"
    ORJSON = "orjson"
    PICKLE = "pickle"

def get_collection_type():
    from .file_motor import FileMotorCollection
    return Union[AgnosticCollection, FileMotorCollection]

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
