import asyncio
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

from .file_motor_filtering import get_field_value, compare_values, match_logical_operator, match_condition

from bson import ObjectId

from .types import SerializationFormat, deserialize_data, serialize_data


class FileMotorClient:
    def __init__(self, base_path: Optional[str] = None, serialization_format: SerializationFormat = SerializationFormat.JSON):
        self.serialization_format = serialization_format
        if base_path is None:
            base_path = os.path.join(tempfile.gettempdir(), "uodm_filedb")
        self.base_path = Path(base_path)
        self.current_db: Optional[FileMotorDatabase] = None

    def get_default_database(self) -> "FileMotorDatabase":
        if self.current_db is None:
            self.current_db = FileMotorDatabase(self, "default")
        return self.current_db

    async def list_database_names(self) -> List[str]:
        if not self.base_path.exists():
            return []
        return [d.name for d in self.base_path.iterdir() if d.is_dir()]

    def __getitem__(self, db_name: str) -> "FileMotorDatabase":
        self.current_db = FileMotorDatabase(self, db_name)
        return self.current_db


class FileMotorDatabase:
    def __init__(self, client: FileMotorClient, db_name: str):
        self.client = client
        self.name = db_name
        self.path = client.base_path / db_name

    def get_collection(self, name: str, **_) -> "FileMotorCollection":
        return FileMotorCollection(self, name)


class FileMotorCollection:
    def __init__(self, database: FileMotorDatabase, name: str):
        self.database = database
        self.name = name
        self.path = database.path / name
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.mkdir(exist_ok=True)
        self.use_orjson: bool = False  # Default to False since it's not that common

    async def insert_one(self, document: Dict[str, Any]) -> "InsertOneResult":
        if "_id" not in document:
            document["_id"] = str(ObjectId())

        file_path = self.path / f"{document['_id']}.json"
        async with asyncio.Lock():
            with open(file_path, "wb") as f:
                f.write(serialize_data(document, self.database.client.serialization_format))

        return InsertOneResult(document["_id"])

    async def find_one(self, filter_dict: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if "_id" in filter_dict:
            file_path = self.path / f"{filter_dict['_id']}.json"
            if file_path.exists():
                with open(file_path, "rb") as f:
                    return deserialize_data(f.read(), self.database.client.serialization_format)
            return None

        # Simple filtering for other fields
        for file_path in self.path.glob("*.json"):
            with open(file_path, "rb") as f:
                doc = deserialize_data(f.read(), self.database.client.serialization_format)
                if all(doc.get(k) == v for k, v in filter_dict.items()):
                    return doc
        return None

    def find(self, filter_dict: Dict[str, Any]) -> "FileMotorCursor":
        return FileMotorCursor(self, filter_dict)


    async def update_one(self, filter_dict: Dict[str, Any], update_dict: Dict[str, Any], upsert: bool = False) -> "UpdateResult":
        doc = await self.find_one(filter_dict)
        if doc is None:
            if upsert:
                new_doc = {**filter_dict, **update_dict.get("$set", {})}
                await self.insert_one(new_doc)
                return UpdateResult(1, 1)
            return UpdateResult(0, 0)

        if "$set" in update_dict:
            doc.update(update_dict["$set"])
            file_path = self.path / f"{doc['_id']}.json"
            async with asyncio.Lock():
                with open(file_path, "wb") as f:
                    f.write(serialize_data(doc, self.database.client.serialization_format))
            return UpdateResult(1, 1)
        return UpdateResult(0, 0)

    async def update_many(self, filter_dict: Dict[str, Any], update_dict: Dict[str, Any]) -> "UpdateResult":
        modified_count = 0
        matched_count = 0

        # Handle special case for updating by _id list
        if "_id" in filter_dict and isinstance(filter_dict["_id"], dict) and "$in" in filter_dict["_id"]:
            id_list = filter_dict["_id"]["$in"]
            for doc_id in id_list:
                file_path = self.path / f"{doc_id}.json"
                if file_path.exists():
                    matched_count += 1
                    with open(file_path, "rb") as f:
                        doc = deserialize_data(f.read(), self.database.client.serialization_format)
                    if "$set" in update_dict:
                        doc.update(update_dict["$set"])
                        with open(file_path, "wb") as f:
                            f.write(serialize_data(doc, self.database.client.serialization_format))
                        modified_count += 1
            return UpdateResult(matched_count, modified_count)

        # Handle general case
        for file_path in self.path.glob("*.json"):
            with open(file_path, "rb") as f:
                doc = deserialize_data(f.read(), self.database.client.serialization_format)

            if all(match_condition(doc.get(k), v) for k, v in filter_dict.items()):
                matched_count += 1
                if "$set" in update_dict:
                    doc.update(update_dict["$set"])
                    with open(file_path, "wb") as f:
                        f.write(serialize_data(doc, self.database.client.serialization_format))
                    modified_count += 1

        return UpdateResult(matched_count, modified_count)

    async def delete_one(self, filter_dict: Dict[str, Any]) -> "DeleteResult":
        doc = await self.find_one(filter_dict)
        if doc is None:
            return DeleteResult(0)

        file_path = self.path / f"{doc['_id']}.json"
        file_path.unlink()
        return DeleteResult(1)

    async def create_index(self, keys: List[str], **kwargs):
        # Simplified index creation - just store index info in a metadata file
        index_path = self.path / "_indexes.json"
        indexes = {}
        if index_path.exists():
            with open(index_path) as f:
                indexes = json.load(f)

        index_name = kwargs.get("name", "_".join(keys) + "_idx")
        indexes[index_name] = {"keys": keys, "options": kwargs}

        with open(index_path, "wb") as f:
            f.write(serialize_data(indexes, self.database.client.serialization_format))


class FileMotorCursor:
    def __init__(self, collection: FileMotorCollection, filter_dict: Dict[str, Any]):
        self.collection = collection
        self.filter_dict = filter_dict
        self._sort_key = None
        self._sort_direction = 1
        self._limit = None
        self._skip = 0

    def sort(self, key_direction: List[tuple]) -> "FileMotorCursor":
        if key_direction:
            self._sort_key = key_direction[0][0]
            self._sort_direction = key_direction[0][1]
        return self

    def limit(self, limit: int) -> "FileMotorCursor":
        self._limit = limit
        return self

    def skip(self, skip: int) -> "FileMotorCursor":
        self._skip = skip
        return self

    async def __aiter__(self):
        matched = 0
        yielded = 0

        files = list(self.collection.path.glob("*.json"))
        if self._sort_key:
            files.sort(
                key=lambda f: deserialize_data(f.read_bytes(), self.collection.database.client.serialization_format)[
                    self._sort_key
                ]
            )
            if self._sort_direction == -1:
                files.reverse()
        else:
            files.sort()

        for file_path in files:
            with open(file_path, "rb") as f:
                doc = deserialize_data(f.read(), self.collection.database.client.serialization_format)
                # Check if we have logical operators
                if any(k.startswith('$') for k in self.filter_dict.keys()):
                    if match_condition(doc, self.filter_dict):
                        matched += 1
                        if matched > self._skip:
                            if self._limit is not None and yielded >= self._limit:
                                break
                            yield doc
                            yielded += 1
                else:
                    # Regular field matching
                    if all(match_condition(doc, v, k) for k, v in self.filter_dict.items()):
                        matched += 1
                        if matched > self._skip:
                            if self._limit is not None and yielded >= self._limit:
                                break
                            yield doc
                            yielded += 1

    async def to_list(self, length: Optional[int] = None) -> List[Dict[str, Any]]:
        result = []
        async for doc in self:
            result.append(doc)
        return result


class InsertOneResult:
    def __init__(self, inserted_id: str):
        self.inserted_id = inserted_id


class UpdateResult:
    def __init__(self, matched_count: int, modified_count: int):
        self.matched_count = matched_count
        self.modified_count = modified_count


class DeleteResult:
    def __init__(self, deleted_count: int):
        self.deleted_count = deleted_count
