import asyncio
import importlib.util
import json
import os
import sqlite3
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union, TYPE_CHECKING

# Define a type alias for aiosqlite.Connection that works with or without aiosqlite
if TYPE_CHECKING:
    from typing import Protocol
    class AiosqliteConnection(Protocol):
        async def close(self) -> None: ...
        async def execute(self, sql: str, parameters: Any = ...) -> Any: ...
        async def commit(self) -> None: ...
        def cursor(self) -> Any: ...

try:
    aiosqlite = importlib.import_module("aiosqlite")
    HAS_AIOSQLITE = True
except ImportError:
    HAS_AIOSQLITE = False
    # Create a module-like object with Connection attribute for type checking
    class DummyConnection:
        pass
    
    class DummyAioSQLite:
        Connection = DummyConnection
        
        @staticmethod
        async def connect(*args, **kwargs):
            raise ImportError("aiosqlite package is required for SQLite support")
        
    aiosqlite = DummyAioSQLite()  # type: ignore

from bson import ObjectId

from .types import SerializationFormat, deserialize_data, serialize_data


class SQLiteMotorClient:
    def __init__(self, db_path: Optional[str] = None, **kwargs):
        if not HAS_AIOSQLITE:
            raise ImportError("aiosqlite package is required for SQLite support. Install it with: pip install 'uodm[sqlite]'")

        if db_path is None:
            db_path = os.path.join(tempfile.gettempdir(), "uodm_sqlite.db")

        # Define db_path at declaration time
        self.db_path: Union[str, Path]

        # Handle in-memory database special case
        if db_path == ":memory:":
            self.db_path = db_path  # str type
        else:
            self.db_path = Path(db_path)  # Path type
            # Ensure parent directory exists
            if not self.db_path.parent.exists():
                self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self.connection_params = kwargs
        self.current_db: Optional[SQLiteMotorDatabase] = None
        self._connections: Dict[str, Any] = {}

    async def _get_connection(self, db_name: str) -> Any:
        """Get or create a database connection for the specified database name"""
        if db_name not in self._connections:
            # For SQLite, the database name is typically just used as a namespace
            # as there's usually one database file per connection
            if self.db_path == ":memory:":
                conn = await aiosqlite.connect(":memory:", **self.connection_params)
            else:
                db_path = self.db_path if isinstance(self.db_path, str) else str(self.db_path)
                conn = await aiosqlite.connect(db_path, **self.connection_params)

            # Enable foreign keys and journal mode WAL for better concurrency
            await conn.execute("PRAGMA foreign_keys = ON")
            await conn.execute("PRAGMA journal_mode = WAL")

            # Store connection
            self._connections[db_name] = conn

        return self._connections[db_name]

    def get_default_database(self) -> "SQLiteMotorDatabase":
        if self.current_db is None:
            self.current_db = SQLiteMotorDatabase(self, "default")
        return self.current_db

    async def list_database_names(self) -> List[str]:
        # For SQLite, we'll consider each table prefix as a "database"
        if self.db_path == ":memory:":
            # For in-memory database, we need an active connection
            conn = await self._get_connection("default")
            cursor = await conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            tables = await cursor.fetchall()
            # Extract database names (table prefixes before '_')
            db_names = set()
            for (table_name,) in tables:
                if "_" in table_name:
                    db_name = table_name.split("_")[0]
                    db_names.add(db_name)
            return list(db_names)
        else:
            # Check if database file exists
            if isinstance(self.db_path, Path) and not self.db_path.exists():
                return []

            # Get the list of tables and extract database names
            conn = await self._get_connection("default")
            cursor = await conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            tables = await cursor.fetchall()

            # Extract database names (table prefixes before '_')
            db_names = set()
            for (table_name,) in tables:
                if "_" in table_name:
                    db_name = table_name.split("_")[0]
                    db_names.add(db_name)

            if not db_names:
                db_names.add("default")

            return list(db_names)

    def __getitem__(self, db_name: str) -> "SQLiteMotorDatabase":
        self.current_db = SQLiteMotorDatabase(self, db_name)
        return self.current_db

    async def close(self):
        """Close all database connections"""
        for conn in self._connections.values():
            await conn.close()
        self._connections.clear()


class SQLiteMotorDatabase:
    def __init__(self, client: SQLiteMotorClient, db_name: str):
        self.client = client
        self.name = db_name

    def get_collection(self, name: str, **_) -> "SQLiteMotorCollection":
        return SQLiteMotorCollection(self, name)

    async def get_connection(self) -> Any:
        """Get the database connection for this database"""
        return await self.client._get_connection(self.name)


class SQLiteMotorCollection:
    def __init__(self, database: SQLiteMotorDatabase, name: str):
        self.database = database
        self.name = name
        self.table_name = f"{database.name}_{name}"
        self.metadata_table_name = f"{database.name}_{name}_metadata"
        self._lock = asyncio.Lock()
        self._initialized = False

    async def _initialize(self):
        """Initialize the collection tables if they don't exist"""
        async with self._lock:
            if self._initialized:
                return

            conn = await self.database.get_connection()

            # Create the main document table if it doesn't exist
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS "{self.table_name}" (
                    _id TEXT PRIMARY KEY,
                    doc BLOB NOT NULL
                )
            """)

            # Create the metadata table for indexes if it doesn't exist
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS "{self.metadata_table_name}" (
                    key TEXT PRIMARY KEY,
                    value BLOB NOT NULL
                )
            """)

            await conn.commit()
            self._initialized = True

    async def insert_one(self, document: Dict[str, Any]) -> "InsertOneResult":
        await self._initialize()

        if "_id" not in document:
            document["_id"] = str(ObjectId())

        doc_id = document["_id"]
        doc_data = serialize_data(document, SerializationFormat.JSON)

        conn = await self.database.get_connection()
        async with self._lock:
            await conn.execute(f'INSERT INTO "{self.table_name}" (_id, doc) VALUES (?, ?)', (doc_id, doc_data))
            await conn.commit()

        return InsertOneResult(doc_id)

    async def find_one(self, filter_dict: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        await self._initialize()

        # Special case for direct _id lookup
        if "_id" in filter_dict and len(filter_dict) == 1:
            doc_id = filter_dict["_id"]

            # Convert ObjectId to string if needed
            if hasattr(doc_id, "__str__"):
                doc_id = str(doc_id)

            conn = await self.database.get_connection()
            cursor = await conn.execute(f'SELECT doc FROM "{self.table_name}" WHERE _id = ?', (doc_id,))
            row = await cursor.fetchone()

            if row:
                return deserialize_data(row[0], SerializationFormat.JSON)
            return None

        # For other filters, convert to a query
        query, params = self._build_query(filter_dict)

        conn = await self.database.get_connection()
        cursor = await conn.execute(f'SELECT doc FROM "{self.table_name}" {query} LIMIT 1', params)
        row = await cursor.fetchone()

        if row:
            return deserialize_data(row[0], SerializationFormat.JSON)
        return None

    def find(self, filter_dict: Dict[str, Any]) -> "SQLiteMotorCursor":
        return SQLiteMotorCursor(self, filter_dict)

    async def update_one(self, filter_dict: Dict[str, Any], update_dict: Dict[str, Any], upsert: bool = False) -> "UpdateResult":
        await self._initialize()

        # Find the document to update
        doc = await self.find_one(filter_dict)

        if doc is None:
            if upsert:
                # Create a new document with the filter and update data
                new_doc = {}
                new_doc.update(filter_dict)
                if "$set" in update_dict:
                    new_doc.update(update_dict["$set"])
                await self.insert_one(new_doc)
                return UpdateResult(0, 1)
            return UpdateResult(0, 0)

        # Apply the updates
        if "$set" in update_dict:
            doc.update(update_dict["$set"])

            # Save the updated document
            doc_id = doc["_id"]

            # Convert ObjectId to string if needed
            if hasattr(doc_id, "__str__"):
                doc_id = str(doc_id)

            doc_data = serialize_data(doc, SerializationFormat.JSON)

            conn = await self.database.get_connection()
            async with self._lock:
                await conn.execute(f'UPDATE "{self.table_name}" SET doc = ? WHERE _id = ?', (doc_data, doc_id))
                await conn.commit()

            return UpdateResult(1, 1)

        return UpdateResult(1, 0)

    async def update_many(self, filter_dict: Dict[str, Any], update_dict: Dict[str, Any]) -> "UpdateResult":
        await self._initialize()

        if "$set" not in update_dict:
            return UpdateResult(0, 0)

        # Special case for updating by _id list
        if "_id" in filter_dict and isinstance(filter_dict["_id"], dict) and "$in" in filter_dict["_id"]:
            id_list = filter_dict["_id"]["$in"]
            matched_count = 0
            modified_count = 0

            for doc_id in id_list:
                # Find document
                conn = await self.database.get_connection()
                cursor = await conn.execute(f'SELECT doc FROM "{self.table_name}" WHERE _id = ?', (doc_id,))
                row = await cursor.fetchone()

                if row:
                    matched_count += 1
                    doc = deserialize_data(row[0], SerializationFormat.JSON)
                    doc.update(update_dict["$set"])

                    # Save updated document
                    doc_data = serialize_data(doc, SerializationFormat.JSON)
                    async with self._lock:
                        await conn.execute(f'UPDATE "{self.table_name}" SET doc = ? WHERE _id = ?', (doc_data, doc_id))
                        await conn.commit()
                    modified_count += 1

            return UpdateResult(matched_count, modified_count)

        # For other filters, convert to a query
        query, params = self._build_query(filter_dict)

        # Get all matching documents
        conn = await self.database.get_connection()
        cursor = await conn.execute(f'SELECT _id, doc FROM "{self.table_name}" {query}', params)
        rows = await cursor.fetchall()

        matched_count = len(list(rows))
        modified_count = 0

        # Update each document
        for doc_id, doc_data in rows:
            doc = deserialize_data(doc_data, SerializationFormat.JSON)
            doc.update(update_dict["$set"])

            # Save updated document
            new_doc_data = serialize_data(doc, SerializationFormat.JSON)
            async with self._lock:
                await conn.execute(f'UPDATE "{self.table_name}" SET doc = ? WHERE _id = ?', (new_doc_data, doc_id))
            modified_count += 1

        # Commit all changes
        await conn.commit()

        return UpdateResult(matched_count, modified_count)

    async def delete_one(self, filter_dict: Dict[str, Any]) -> "DeleteResult":
        await self._initialize()

        # Special case for direct _id lookup
        if "_id" in filter_dict and len(filter_dict) == 1:
            doc_id = filter_dict["_id"]

            # Convert ObjectId to string if needed
            if hasattr(doc_id, "__str__"):
                doc_id = str(doc_id)

            conn = await self.database.get_connection()
            async with self._lock:
                cursor = await conn.execute(f'DELETE FROM "{self.table_name}" WHERE _id = ?', (doc_id,))
                await conn.commit()

            return DeleteResult(cursor.rowcount)

        # For other filters, find the document first
        doc = await self.find_one(filter_dict)
        if doc is None:
            return DeleteResult(0)

        # Delete the document
        doc_id = doc["_id"]

        # Convert ObjectId to string if needed
        if hasattr(doc_id, "__str__"):
            doc_id = str(doc_id)

        conn = await self.database.get_connection()
        async with self._lock:
            cursor = await conn.execute(f'DELETE FROM "{self.table_name}" WHERE _id = ?', (doc_id,))
            await conn.commit()

        return DeleteResult(cursor.rowcount)

    async def create_index(self, keys: List[str], **kwargs):
        await self._initialize()

        # Store index information in the metadata table
        index_name = kwargs.get("name", "_".join(keys) + "_idx")
        index_data = {"keys": keys, "options": kwargs}

        # Serialize the index data
        index_value = serialize_data(index_data, SerializationFormat.JSON)

        # Store in the metadata table
        conn = await self.database.get_connection()
        async with self._lock:
            await conn.execute(
                f'INSERT OR REPLACE INTO "{self.metadata_table_name}" (key, value) VALUES (?, ?)', (f"index:{index_name}", index_value)
            )
            await conn.commit()

        # Create actual SQLite indexes for basic field lookups
        # This provides actual performance benefits for queries
        for key in keys:
            # Index the JSON field using SQLite's JSON functions
            # We're creating a functional index on a JSON path inside the document
            index_sql = f'''
                CREATE INDEX IF NOT EXISTS "{self.table_name}_{key}_idx"
                ON "{self.table_name}" (json_extract(doc, '$.{key}'))
            '''
            try:
                await conn.execute(index_sql)
                await conn.commit()
            except sqlite3.OperationalError:
                # Skip if this JSON extraction isn't supported by the SQLite version
                pass

    def _build_query(self, filter_dict: Dict[str, Any]) -> Tuple[str, List[Any]]:
        """Convert a MongoDB-style filter to a SQLite query"""
        if not filter_dict:
            return "", []

        conditions = []
        params = []

        for key, value in filter_dict.items():
            # Handle direct _id match
            if key == "_id":
                conditions.append("_id = ?")
                params.append(value)
                continue

            # Handle complex query operators
            if isinstance(value, dict) and any(k.startswith("$") for k in value.keys()):
                for op, op_value in value.items():
                    if op == "$eq":
                        conditions.append(f"json_extract(doc, '$.{key}') = json(?)")
                        params.append(json.dumps(op_value))
                    elif op == "$ne":
                        conditions.append(f"json_extract(doc, '$.{key}') != json(?)")
                        params.append(json.dumps(op_value))
                    elif op == "$gt":
                        # For numeric comparisons, we need to cast
                        if isinstance(op_value, (int, float)):
                            conditions.append(f"CAST(json_extract(doc, '$.{key}') AS NUMERIC) > ?")
                            params.append(op_value)
                        else:
                            conditions.append(f"json_extract(doc, '$.{key}') > json(?)")
                            params.append(json.dumps(op_value))
                    elif op == "$gte":
                        if isinstance(op_value, (int, float)):
                            conditions.append(f"CAST(json_extract(doc, '$.{key}') AS NUMERIC) >= ?")
                            params.append(op_value)
                        else:
                            conditions.append(f"json_extract(doc, '$.{key}') >= json(?)")
                            params.append(json.dumps(op_value))
                    elif op == "$lt":
                        if isinstance(op_value, (int, float)):
                            conditions.append(f"CAST(json_extract(doc, '$.{key}') AS NUMERIC) < ?")
                            params.append(op_value)
                        else:
                            conditions.append(f"json_extract(doc, '$.{key}') < json(?)")
                            params.append(json.dumps(op_value))
                    elif op == "$lte":
                        if isinstance(op_value, (int, float)):
                            conditions.append(f"CAST(json_extract(doc, '$.{key}') AS NUMERIC) <= ?")
                            params.append(op_value)
                        else:
                            conditions.append(f"json_extract(doc, '$.{key}') <= json(?)")
                            params.append(json.dumps(op_value))
                    elif op == "$in":
                        placeholders = ", ".join(["?"] * len(op_value))
                        conditions.append(f"json_extract(doc, '$.{key}') IN ({placeholders})")
                        params.extend([json.dumps(x) for x in op_value])
                    elif op == "$nin":
                        placeholders = ", ".join(["?"] * len(op_value))
                        conditions.append(f"json_extract(doc, '$.{key}') NOT IN ({placeholders})")
                        params.extend([json.dumps(x) for x in op_value])
            else:
                # Simple equality match
                if isinstance(value, str):
                    # For string values, use direct string comparison (not json function)
                    conditions.append(f"json_extract(doc, '$.{key}') = ?")
                    params.append(value)
                elif isinstance(value, (int, float)):
                    # For numbers, cast to ensure numeric comparison
                    conditions.append(f"CAST(json_extract(doc, '$.{key}') AS NUMERIC) = ?")
                    params.append(value)
                else:
                    # For other types, use json comparison
                    conditions.append(f"json_extract(doc, '$.{key}') = json(?)")
                    params.append(json.dumps(value))

        if conditions:
            return f"WHERE {' AND '.join(conditions)}", params
        return "", []


class SQLiteMotorCursor:
    def __init__(self, collection: SQLiteMotorCollection, filter_dict: Dict[str, Any]):
        self.collection = collection
        self.filter_dict = filter_dict
        self._sort_key = None
        self._sort_direction = 1
        self._limit: Optional[int] = None
        self._skip: int = 0

    def sort(self, key_direction: List[tuple]) -> "SQLiteMotorCursor":
        if key_direction:
            self._sort_key = key_direction[0][0]
            self._sort_direction = key_direction[0][1]
        return self

    def limit(self, limit: int) -> "SQLiteMotorCursor":
        self._limit = limit
        return self

    def skip(self, skip: int) -> "SQLiteMotorCursor":
        self._skip = skip
        return self

    async def _build_full_query(self) -> Tuple[str, List[Any]]:
        """Build the complete SQL query with sorting, limit, and offset"""
        query, params = self.collection._build_query(self.filter_dict)

        # Add sorting
        if self._sort_key:
            direction = "DESC" if self._sort_direction == -1 else "ASC"
            query += f" ORDER BY json_extract(doc, '$.{self._sort_key}') {direction}"

        # Add limit and offset
        if self._limit is not None:
            query += f" LIMIT {self._limit}"

        if self._skip:
            query += f" OFFSET {self._skip}"

        return query, params

    async def __aiter__(self):
        await self.collection._initialize()

        query, params = await self._build_full_query()

        conn = await self.collection.database.get_connection()
        cursor = await conn.execute(f'SELECT doc FROM "{self.collection.table_name}" {query}', params)

        async for row in cursor:
            yield deserialize_data(row[0], SerializationFormat.JSON)

    async def to_list(self, length: Optional[int] = None) -> List[Dict[str, Any]]:
        await self.collection._initialize()

        # If length is provided, adjust the limit
        original_limit = self._limit
        if length is not None:
            self._limit = length

        query, params = await self._build_full_query()

        # Reset the limit
        self._limit = original_limit

        conn = await self.collection.database.get_connection()
        cursor = await conn.execute(f'SELECT doc FROM "{self.collection.table_name}" {query}', params)
        rows = await cursor.fetchall()

        return [deserialize_data(row[0], SerializationFormat.JSON) for row in rows]


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
