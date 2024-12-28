import sys
import tempfile

import mongomock
import pytest
from motor.motor_asyncio import AsyncIOMotorClient

sys.path.append("src")

from uodm import UODM, Collection, Field
from uodm.file_motor import FileMotorClient


@pytest.fixture
def mock_motor_client():
    return mongomock.MongoClient()


@pytest.fixture
def database_url():
    return "mongodb://localhost:27017/test_db"


@pytest.mark.asyncio
async def test_uodm_initialization_and_connection(database_url):
    uodm = UODM(database_url, connect_now=False)
    assert uodm.mongo is None

    uodm.connect()
    assert isinstance(uodm.mongo, AsyncIOMotorClient)
    assert uodm.database is not None

    await uodm.close()
    assert isinstance(uodm.mongo, AsyncIOMotorClient)


@pytest.mark.asyncio
async def test_set_db_with_existing_db(mock_motor_client):
    uodm = UODM(mock_motor_client, connect_now=False)
    uodm.apply_connection(mock_motor_client)

    await uodm.set_db("test_db")
    assert uodm.database is not None
    assert uodm.database.name == "test_db"


@pytest.mark.asyncio
async def test_set_db_with_non_existent_db(mock_motor_client):
    uodm = UODM(mock_motor_client, connect_now=False)
    uodm.apply_connection(mock_motor_client)

    with pytest.raises(ValueError) as exc_info:
        await uodm.set_db("non_existent_db", check_exist=True)
    assert str(exc_info.value) == "Database non_existent_db not found"


@pytest.mark.asyncio
async def test_collection_save_and_retrieve(mock_motor_client):
    class SampleCollection(Collection):
        __collection__ = "sample_collection"

        name: str = Field(...)
        age: int = Field(...)

    uodm = UODM(mock_motor_client, connect_now=False)
    uodm.apply_connection(mock_motor_client)

    sample = SampleCollection(name="John Doe", age=30)
    await sample.save()

    retrieved = await SampleCollection.get(_id=sample._id)
    assert retrieved is not None
    assert retrieved.name == "John Doe"
    assert retrieved.age == 30


@pytest.mark.asyncio
async def test_collection_update(mock_motor_client):
    class SampleCollection(Collection):
        __collection__ = "sample_collection"

        name: str = Field(...)
        age: int = Field(...)

    uodm = UODM(mock_motor_client, connect_now=False)
    uodm.apply_connection(mock_motor_client)

    sample = SampleCollection(name="John Doe", age=30)
    await sample.save()

    await SampleCollection.update([sample], name="Jane Doe")

    updated = await SampleCollection.get(_id=sample._id)
    assert updated is not None
    assert updated.name == "Jane Doe"
    assert updated.age == 30


@pytest.mark.asyncio
async def test_collection_delete(mock_motor_client):
    class SampleCollection(Collection):
        __collection__ = "sample_collection"

        name: str = Field(...)
        age: int = Field(...)

    uodm = UODM(mock_motor_client, connect_now=False)
    uodm.apply_connection(mock_motor_client)

    sample = SampleCollection(name="John Doe", age=30)
    await sample.save()
    assert await SampleCollection.get(_id=sample._id) is not None

    await sample.delete()
    assert await SampleCollection.get(_id=sample._id) is None
