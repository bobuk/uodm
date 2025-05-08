import asyncio
import time
from enum import Enum
from typing import Any, Awaitable, Callable, Dict, Generic, List, Optional, Set, TypeVar

from bson import Timestamp

T = TypeVar("T")


class ChangeType(str, Enum):
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    REPLACE = "replace"
    DROP = "drop"
    RENAME = "rename"
    INVALIDATE = "invalidate"


class ChangeStreamDocument(Dict[str, Any]):
    """Represents a change stream document with standard fields."""

    @property
    def operation_type(self) -> str:
        """Get the type of operation that occurred."""
        return self.get("operationType", "")

    @property
    def document_key(self) -> Dict[str, Any]:
        """Get the _id of the document that was changed."""
        return self.get("documentKey", {})

    @property
    def full_document(self) -> Optional[Dict[str, Any]]:
        """Get the most current majority-committed version of the document."""
        return self.get("fullDocument")

    @property
    def ns(self) -> Dict[str, str]:
        """Get the namespace (database and collection) affected by the change."""
        return self.get("ns", {})

    @property
    def update_description(self) -> Dict[str, Any]:
        """Get description of the update (for update operations)."""
        return self.get("updateDescription", {})

    @property
    def resume_token(self) -> Any:
        """Get a token that can be used to resume a change stream."""
        return self.get("_id")

    @property
    def cluster_time(self) -> Optional[Timestamp]:
        """Get the timestamp from the oplog entry."""
        return self.get("clusterTime")


class ChangeStream(Generic[T]):
    """Common interface for change streams, regardless of the backend."""

    def __init__(self) -> None:
        self._closed = False
        self._listening_task: Optional[asyncio.Task[Any]] = None
        self._handlers: List[Callable[[ChangeStreamDocument], Awaitable[None]]] = []

    async def next(self) -> Optional[ChangeStreamDocument]:
        """Get the next available change document."""
        raise NotImplementedError("Subclasses must implement this method")

    async def close(self):
        """Close the change stream."""
        if not self._closed:
            self._closed = True
            if self._listening_task:
                self._listening_task.cancel()
                try:
                    await self._listening_task
                except asyncio.CancelledError:
                    pass

    async def watch(self, handler: Callable[[ChangeStreamDocument], Awaitable[None]]):
        """Add a handler to process change stream events."""
        self._handlers.append(handler)
        if not self._listening_task:
            self._listening_task = asyncio.create_task(self._listener())

    async def _listener(self):
        """Background task that processes change stream events."""
        try:
            while not self._closed:
                try:
                    doc = await self.next()
                    if doc is None:
                        continue

                    # Process with all handlers
                    for handler in self._handlers:
                        try:
                            await handler(doc)
                        except Exception as e:
                            # Log error but continue processing
                            print(f"Error in change stream handler: {e}")
                except Exception as e:
                    # Log error but continue listening
                    print(f"Error in change stream: {e}")
                    await asyncio.sleep(1)  # Prevent tight loop on errors
        except asyncio.CancelledError:
            # Normal cancellation
            pass
        except Exception as e:
            print(f"Unhandled error in change stream listener: {e}")
        finally:
            self._closed = True


class MongoChangeStream(ChangeStream[T]):
    """MongoDB native change stream implementation."""

    def __init__(self, native_stream: Any) -> None:
        super().__init__()
        self._stream = native_stream
        self._change_queue: asyncio.Queue[ChangeStreamDocument] = asyncio.Queue()
        self._streaming_task: Optional[asyncio.Task[Any]] = None

        # Start the background streaming task
        self._start_streaming()

    def _start_streaming(self) -> None:
        """Start background task to process MongoDB stream"""
        if not self._streaming_task:
            self._streaming_task = asyncio.create_task(self._stream_processor())

    async def _stream_processor(self):
        """Background task to process the MongoDB change stream"""
        try:
            async with self._stream as stream:
                async for change in stream:
                    if self._closed:
                        break
                    await self._change_queue.put(ChangeStreamDocument(change))
        except Exception as e:
            print(f"Error in MongoDB change stream: {e}")
        finally:
            self._closed = True

    async def next(self) -> Optional[ChangeStreamDocument]:
        """Get the next change from the stream"""
        if self._closed:
            return None

        try:
            # Wait for the next change with a timeout
            return await asyncio.wait_for(self._change_queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            return None

    async def close(self):
        """Close the MongoDB change stream."""
        await super().close()
        if self._streaming_task:
            self._streaming_task.cancel()
            try:
                await self._streaming_task
            except asyncio.CancelledError:
                pass


class PollingChangeStream(ChangeStream[T]):
    """Polling-based change stream implementation for non-MongoDB backends."""

    def __init__(
        self,
        collection,  # The collection to poll (FileMotorCollection or SQLiteMotorCollection)
        poll_interval: float = 1.0,
        filter_dict: Optional[Dict[str, Any]] = None,
    ):
        super().__init__()
        self._collection = collection
        self._poll_interval = poll_interval
        self._filter = filter_dict or {}
        self._seen_ids: Set[str] = set()
        self._last_poll_time: float = 0
        self._queue: asyncio.Queue[ChangeStreamDocument] = asyncio.Queue()
        self._polling_task: Optional[asyncio.Task[Any]] = None

    async def _start_polling(self):
        """Start the background polling task."""
        if not self._polling_task:
            self._polling_task = asyncio.create_task(self._poll_loop())

    async def _poll_loop(self):
        """Background task that polls for changes."""
        try:
            # Initial fetch to establish baseline
            await self._fetch_current_state()

            while not self._closed:
                await asyncio.sleep(self._poll_interval)
                await self._check_for_changes()
        except asyncio.CancelledError:
            # Normal cancellation
            pass
        except Exception as e:
            print(f"Error in polling loop: {e}")
        finally:
            self._closed = True

    async def _fetch_current_state(self):
        """Fetch the current state of the collection to establish a baseline."""
        cursor = self._collection.find(self._filter)
        docs = await cursor.to_list(None)
        for doc in docs:
            doc_id = str(doc.get("_id", ""))
            if doc_id:
                self._seen_ids.add(doc_id)
        self._last_poll_time = time.time()

    async def _check_for_changes(self):
        """Check for changes since the last poll."""
        current_time = time.time()

        # Get current state
        cursor = self._collection.find(self._filter)
        current_docs = await cursor.to_list(None)
        current_ids = {str(doc.get("_id", "")) for doc in current_docs if doc.get("_id")}

        # Find new docs (inserts)
        new_ids = current_ids - self._seen_ids
        for doc in current_docs:
            doc_id = str(doc.get("_id", ""))
            if doc_id in new_ids:
                change_event = self._create_change_event(ChangeType.INSERT, doc)
                await self._queue.put(change_event)

        # Find removed docs (deletes)
        deleted_ids = self._seen_ids - current_ids
        for doc_id in deleted_ids:
            change_event = self._create_change_event(ChangeType.DELETE, {"_id": doc_id}, include_full_doc=False)
            await self._queue.put(change_event)

        # Update our tracking state
        self._seen_ids = current_ids
        self._last_poll_time = current_time

    def _create_change_event(self, op_type: ChangeType, document: Dict[str, Any], include_full_doc: bool = True) -> ChangeStreamDocument:
        """Create a change event document similar to MongoDB's format."""
        doc_id = document.get("_id")
        event = {
            "operationType": op_type.value,
            "documentKey": {"_id": doc_id},
            "ns": {"db": getattr(self._collection.database, "name", "default"), "coll": self._collection.name},
            "wallTime": time.time(),
        }

        # Include the full document for inserts and updates
        if include_full_doc and op_type in (ChangeType.INSERT, ChangeType.UPDATE):
            event["fullDocument"] = document

        return ChangeStreamDocument(event)

    async def next(self) -> Optional[ChangeStreamDocument]:
        """Get the next change event from the queue."""
        if self._closed:
            return None

        # Start polling if not already started
        await self._start_polling()

        try:
            # Wait for the next change with a timeout
            return await asyncio.wait_for(self._queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            return None

    async def close(self):
        """Close the polling change stream."""
        await super().close()
        if self._polling_task:
            self._polling_task.cancel()
            try:
                await self._polling_task
            except asyncio.CancelledError:
                pass
