import importlib.metadata

from bson import ObjectId
from motor.core import AgnosticClient, AgnosticCollection, AgnosticDatabase  # noqa
from pydantic import BaseModel, Field  # noqa
from pymongo.errors import ConfigurationError, DuplicateKeyError, PyMongoError  # noqa

from .uodm import UODM, Collection, EmbeddedModel, Idx, IdxOpts
from .change_streams import ChangeStream, ChangeStreamDocument, ChangeType

__all__ = [
    "AgnosticCollection",
    "AgnosticDatabase",
    "AgnosticClient",
    "BaseModel",
    "Field",
    "EmbeddedModel",
    "ConfigurationError",
    "DuplicateKeyError",
    "PyMongoError",
    "ObjectId",
    "UODM",
    "IdxOpts",
    "Idx",
    "Collection",
    "ChangeStream",
    "ChangeStreamDocument",
    "ChangeType",
]

__version__ = importlib.metadata.version(__name__)
