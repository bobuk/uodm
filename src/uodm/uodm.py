import re
from typing import Generic, List, Type, TypedDict, TypeVar, Optional  # noqa
from bson import ObjectId
from motor import motor_asyncio
from motor.core import AgnosticCollection, AgnosticDatabase, AgnosticClient
from pydantic import BaseModel, Field  # noqa
from pymongo.errors import ConfigurationError, DuplicateKeyError, PyMongoError  # noqa

EmbeddedModel = BaseModel


def normalize(s: str) -> str:
    s = s.replace("Collection", "")
    norm = "_".join(re.findall(r"[A-Z](?:[a-z]+|[A-Z]*(?=[A-Z]|$))", s)).lower()
    return norm


T = TypeVar("T", bound="Collection")


class UODM:
    def __init__(self, url: str, connect_now=True):
        self.mongo: Optional[AgnosticClient] = None
        self.database: Optional[AgnosticDatabase] = None
        self.url: str = url
        if connect_now:
            self.connect()

    def connect(self):
        global _CURRENT_DB
        self.mongo = motor_asyncio.AsyncIOMotorClient(self.url)
        try:
            self.database = self.mongo.get_default_database()
        except ConfigurationError:
            pass
        _CURRENT_DB = self

    def apply_connection(self, client: AgnosticClient):
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
    def db(self) -> AgnosticDatabase:
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
                if idx.options is not None:
                    options = idx.options.model_dump(exclude_none=True)
                options["name"] = options.get("name", ("_".join(idx.keys) + "_idx"))
                await collection.create_index(idx.keys, **options)


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
            self._id = result.inserted_id
            return
        dmp = self.model_dump()
        if not self.model_validate(dmp):
            raise ValueError("Model validation failed")
        await collection.update_one(
            {"_id": self._id}, {"$set": self.model_dump()}, upsert=True
        )

    async def delete(self):
        collection = self.get_collection()
        if self._id is not None:
            await collection.delete_one({"_id": self._id})

    @classmethod
    def get_model_config(cls: Type[T]) -> dict:
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
    async def find(cls: Type[T], **kwargs) -> List[T]:
        collection = cls.get_collection()

        kwargs = Collection.filtering(**kwargs)
        data = await collection.find(kwargs).to_list(None)
        return [cls.create(**d) for d in data]

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
    def get_collection(cls) -> AgnosticCollection:
        options = cls.get_model_config()
        name = str(options.get("collection", normalize(cls.__name__)))

        return UODM.get_current().db.get_collection(
            name,
            codec_options=None,
        )

    @staticmethod
    def filtering(**kwargs) -> dict:
        if "id" in kwargs:
            kwargs["_id"] = kwargs.pop("id")
        if "_id" in kwargs:
            if isinstance(kwargs["_id"], str):
                kwargs["_id"] = ObjectId(kwargs["_id"])
        args = kwargs.copy()
        for arg in kwargs.keys():
            if '_in_' in arg or '__' in arg:
                key = arg.replace('_in_', '.').replace('__', '.')
                args[key] = kwargs[arg]
                del args[arg]
        return args
