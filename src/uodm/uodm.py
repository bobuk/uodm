import re
from typing import Any, Dict, Generic, List, Optional, Type, TypeVar, Union  # noqa

import pymongo.errors
from bson import ObjectId
from motor import motor_asyncio
from motor.core import AgnosticClient, AgnosticCollection, AgnosticDatabase
from pydantic import BaseModel
from pydantic import Field as PydanticField

from .file_motor import FileMotorClient, FileMotorCollection, FileMotorDatabase

EmbeddedModel = BaseModel
Field = PydanticField

ConfigurationError = pymongo.errors.ConfigurationError
DuplicateKeyError = pymongo.errors.DuplicateKeyError
PyMongoError = pymongo.errors.PyMongoError


def normalize(s: str) -> str:
    s = s.replace("Collection", "")
    norm = "_".join(re.findall(r"[A-Z](?:[a-z]+|[A-Z]*(?=[A-Z]|$))", s)).lower()
    return norm


T = TypeVar("T", bound="Collection")


class UODM:
    def __init__(self, url_or_client: Union[str, AgnosticClient, FileMotorClient], connect_now=True):
        self.mongo: Optional[Union[AgnosticClient, FileMotorClient]] = None
        self.database: Optional[Union[AgnosticDatabase, FileMotorDatabase]] = None
        self.url_or_client = url_or_client
        if connect_now:
            self.connect()

    def connect(self):
        global _CURRENT_DB
        if isinstance(self.url_or_client, str):
            if self.url_or_client.startswith("file://"):
                path = self.url_or_client[7:]
                self.mongo = FileMotorClient(path)
            else:
                self.mongo = motor_asyncio.AsyncIOMotorClient(self.url_or_client)
        else:
            self.mongo = self.url_or_client
        try:
            self.database = self.mongo.get_default_database()
        except ConfigurationError:
            pass
        _CURRENT_DB = self

    def apply_connection(self, client: Union[AgnosticClient, FileMotorClient]):
        global _CURRENT_DB
        self.mongo = client
        default = client.get_default_database()
        if default is not None:
            self.database = default
        _CURRENT_DB = self

    async def set_db(self, db: str, check_exist=False) -> "UODM":
        if self.mongo is None:
            raise ValueError("MongoDB is not connected")
        bases = await self.mongo.list_database_names()
        if check_exist and db not in bases:
            raise ValueError(f"Database {db} not found")
        self.database = self.mongo[db]
        return self

    async def close(self):
        pass

    @property
    def db(self) -> Union[AgnosticDatabase, FileMotorDatabase]:
        if self.database is None:
            raise ValueError("Database is not connected")
        return self.database

    @staticmethod
    def get_current() -> "UODM":
        global _CURRENT_DB

        if _CURRENT_DB is None:
            raise ValueError("Database is not connected")
        return _CURRENT_DB

    async def setup(self):
        for cls in Collection.__subclasses__():
            collection = cls.get_collection()

            config = cls.get_model_config()
            for idx in config.get("indexes", []):
                options = {}
                if isinstance(idx, dict):
                    keys = idx.get("keys", [])
                    options = {k: v for k, v in idx.items() if k != "keys"}
                else:
                    keys = idx.keys
                    if idx.options is not None:
                        options = idx.options.model_dump(exclude_none=True)
                options["name"] = options.get("name", ("_".join(keys) + "_idx"))
                await collection.create_index(keys, **options)


_CURRENT_DB: Optional[UODM] = None


class IdxOpts(BaseModel):
    name: Optional[str] = None
    unique: Optional[bool] = None
    background: Optional[bool] = None
    sparse: Optional[bool] = None


class Idx(BaseModel):
    keys: List[str]
    options: Optional[IdxOpts] = None

    def __init__(self, keys: List[str] | str, **kwargs) -> None:
        real_keys = keys if isinstance(keys, list) else [keys]
        options = IdxOpts(**kwargs)
        super().__init__(keys=real_keys, options=options)


class Collection(BaseModel, Generic[T]):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._id: Optional[ObjectId] = None

    async def save(self):
        collection = self.get_collection()

        if self._id is None:
            result = await collection.insert_one(self.model_dump())
            self._id = ObjectId(result.inserted_id) if isinstance(result.inserted_id, str) else result.inserted_id
            return
        dmp = self.model_dump()
        if not self.model_validate(dmp):
            raise ValueError("Model validation failed")
        await collection.update_one({"_id": self._id}, {"$set": self.model_dump()}, upsert=True)

    async def delete(self):
        collection = self.get_collection()
        if self._id is not None:
            await collection.delete_one({"_id": self._id})

    @classmethod
    def get_model_config(cls: Type["Collection[T]"]) -> dict:
        res = {}
        if conf_coll := getattr(cls, "__collection__", None):
            res["collection"] = conf_coll
        if conf_indexes := getattr(cls, "__indexes__", None):
            res["indexes"] = conf_indexes
        return res

    @classmethod
    async def get(cls: Type[T], **kwargs) -> Optional[T]:
        collection = cls.get_collection()
        kwargs = Collection.filtering(**kwargs)
        data = await collection.find_one(kwargs)
        if data is None:
            return None
        return cls.create(**data)

    @classmethod
    async def find(
        cls: Type[T], sort: Optional[str] = None, limit: Optional[int] = None, skip: Optional[int] = None, **kwargs
    ) -> List[T]:
        collection = cls.get_collection()

        kwargs = Collection.filtering(**kwargs)
        cursor = collection.find(kwargs)

        if sort is not None:
            if sort.startswith("-"):
                sorting = [(sort[1:], -1)]
            else:
                sorting = [(sort, 1)]
            cursor = cursor.sort(sorting)

        if limit is not None:
            cursor = cursor.limit(limit)
        if skip is not None:
            cursor = cursor.skip(skip)

        data = await cursor.to_list(None)
        return [cls.create(**d) for d in data]

    @classmethod
    async def count(cls: Type[T], **kwargs) -> int:
        collection = cls.get_collection()

        kwargs = Collection.filtering(**kwargs)
        cursor = collection.find(kwargs)
        return len(await cursor.to_list(None))

    @classmethod
    def create(cls: Type[T], **kwargs):
        object_id = kwargs.pop("_id", None)
        if object_id is None:
            raise ValueError("Object _id isn't set")
        result = cls(**kwargs)
        result._id = object_id
        return result

    @classmethod
    async def update(cls: Type[T], items: List[T], **kwargs):
        collection = cls.get_collection()
        kwargs = Collection.filtering(**kwargs)
        for item in items:
            if item._id is None:
                raise ValueError("Object _id isn't set")
        await collection.update_many(
            {"_id": {"$in": [item._id for item in items]}},
            {"$set": kwargs},
        )

    @classmethod
    async def save_all(cls: Type[T], items: List[T]):
        for item in items:
            await item.save()

    @classmethod
    def get_collection(cls) -> Union[AgnosticCollection, FileMotorCollection]:
        options = cls.get_model_config()
        name = str(options.get("collection", normalize(cls.__name__)))

        return UODM.get_current().db.get_collection(
            name,
            codec_options=None,
        )

    @staticmethod
    def filtering(**kwargs) -> Dict[str, Any]:
        if "id" in kwargs:
            kwargs["_id"] = kwargs.pop("id")
        if "_id" in kwargs:
            if isinstance(kwargs["_id"], str):
                kwargs["_id"] = ObjectId(kwargs["_id"])
        args = kwargs.copy()
        for arg in kwargs.keys():
            if "_in_" in arg or "__" in arg:
                key = arg.replace("_in_", ".").replace("__", ".")
                args[key] = kwargs[arg]
                del args[arg]
        return args
