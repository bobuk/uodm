import importlib.metadata
from bson import ObjectId
from motor.core import AgnosticCollection, AgnosticDatabase, AgnosticClient # noqa
from pydantic import BaseModel, Field  # noqa
from pymongo.errors import ConfigurationError, DuplicateKeyError, PyMongoError  # noqa

from .uodm import UODM, IdxOpts, Idx, Collection, EmbeddedModel

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
]

__version__ = importlib.metadata.version(__name__)
