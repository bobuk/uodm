import tempfile
from pathlib import Path

import pytest

from uodm import UODM, Collection, Field


@pytest.fixture
def temp_db_path():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.mark.asyncio
async def test_file_motor_initialization(temp_db_path):
    uodm = UODM(f"file://{temp_db_path}", connect_now=True)
    assert uodm.mongo is not None
    assert uodm.database is not None
    assert Path(temp_db_path).exists()


@pytest.mark.asyncio
async def test_file_motor_collection_operations(temp_db_path):
    class TestCollection(Collection):
        __collection__ = "test_collection"
        name: str = Field(...)
        value: int = Field(...)

    uodm = UODM(f"file://{temp_db_path}", connect_now=True)  # noqa

    # Test insert
    doc = TestCollection(name="test", value=42)
    await doc.save()
    assert doc._id is not None

    # Test get
    retrieved = await TestCollection.get(_id=doc._id)
    assert retrieved is not None, "Document should exist"
    if retrieved:  # Type narrowing for pyright
        assert retrieved.name == "test"
        assert retrieved.value == 42

    # Test update
    await TestCollection.update([retrieved], name="updated")
    updated = await TestCollection.get(_id=doc._id)
    assert updated is not None, "Updated document should exist"
    assert updated and updated.name == "updated"  # Type narrowing for pyright

    # Test delete
    await doc.delete()
    deleted = await TestCollection.get(_id=doc._id)
    assert deleted is None


@pytest.mark.asyncio
async def test_file_motor_find_operations(temp_db_path):
    class TestCollection(Collection):
        __collection__ = "test_collection"
        name: str = Field(...)
        value: int = Field(...)

    uodm = UODM(f"file://{temp_db_path}", connect_now=True)  # noqa

    # Insert test data
    docs = [TestCollection(name=f"test{i}", value=i) for i in range(5)]
    await TestCollection.save_all(docs)

    # Test find with filter
    results = await TestCollection.find(value={"$gt": 2})
    assert len(results) == 2

    # Test find with sort and limit
    results = await TestCollection.find(sort="value", limit=2)
    assert len(results) == 2
    assert results[0].value < results[1].value

    # Test find with skip
    results = await TestCollection.find(skip=2)
    assert len(results) == 3


@pytest.mark.asyncio
async def test_file_motor_index_creation(temp_db_path):
    class TestCollection(Collection):
        __collection__ = "test_collection"
        name: str = Field(...)
        value: int = Field(...)

        __indexes__ = [{"keys": ["name"], "unique": True}]

    uodm = UODM(f"file://{temp_db_path}", connect_now=True)
    await uodm.setup()

    # Verify index file was created
    index_path = Path(temp_db_path) / "default" / "test_collection" / "_indexes.json"
    assert index_path.exists()
