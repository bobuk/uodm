import tempfile
from pathlib import Path

import pytest

from uodm import UODM, Collection, Field
from uodm.file_motor import FileMotorClient, FileMotorCollection, FileMotorDatabase


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

    uodm = UODM(f"file://{temp_db_path}#json", connect_now=True)  # noqa

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
async def test_file_motor_nested_filtering(temp_db_path):
    class TestCollection(Collection):
        __collection__ = "test_collection"
        nested: dict = Field(default_factory=dict)

    uodm = UODM(f"file://{temp_db_path}", connect_now=True)  # noqa

    # Create test documents with nested data
    doc1 = TestCollection(nested={"date": {"time": {"hour": 12}}})
    doc2 = TestCollection(nested={"date": {"time": {"hour": 14}}})
    await TestCollection.save_all([doc1, doc2])

    # Test filtering with dotted notation
    results = await TestCollection.find(nested__date__time__hour=12)
    assert len(results) == 1
    assert results[0].nested["date"]["time"]["hour"] == 12

    # Test filtering with non-existent nested path
    results = await TestCollection.find(nested__invalid__path=12)
    assert len(results) == 0


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

    # Test find with less than filter
    results = await TestCollection.find(value={"$lt": 2})
    assert len(results) == 2

    # Test find with less than or equal filter
    results = await TestCollection.find(value={"$lte": 2})
    assert len(results) == 3

    # Test find with greater than or equal filter
    results = await TestCollection.find(value={"$gte": 3})
    assert len(results) == 2

    # Test find with not equal filter
    results = await TestCollection.find(value={"$ne": 2})
    assert len(results) == 4

    # Test find with in filter
    results = await TestCollection.find(value={"$in": [1, 3]})
    assert len(results) == 2

    # Test find with not in filter
    results = await TestCollection.find(value={"$nin": [1, 3]})
    assert len(results) == 3

    # Test find with exists filter
    results = await TestCollection.find(value={"$exists": True})
    assert len(results) == 5

    results = await TestCollection.find(value={"$exists": False})
    assert len(results) == 0

    # Test dictionary filtering
    results = await TestCollection.find({"value": 3})
    assert len(results) == 1
    assert results[0].value == 3

    # Test combined dictionary and kwargs filtering
    results = await TestCollection.find({"name": "test2"}, value=3)
    assert len(results) == 0

    results = await TestCollection.find({"name": "test3"}, value=3)
    assert len(results) == 1
    assert results[0].name == "test3"
    assert results[0].value == 3

    # Test get with dictionary
    doc = await TestCollection.get({"value": 2})
    assert doc is not None
    assert doc.value == 2
    assert doc.name == "test2"

    # Test count with dictionary
    count = await TestCollection.count({"value": {"$lt": 3}})
    assert count == 3

    # Test find with sort and limit
    results = await TestCollection.find(sort="value", limit=2)
    assert len(results) == 2
    assert results[0].value < results[1].value

    # Test find with skip
    results = await TestCollection.find(skip=2)
    assert len(results) == 3


@pytest.mark.asyncio
async def test_file_motor_logical_operations(temp_db_path):
    class TestCollection(Collection):
        __collection__ = "test_collection"
        name: str = Field(...)
        value: int = Field(...)

    uodm = UODM(f"file://{temp_db_path}", connect_now=True)  # noqa

    # Insert test data
    docs = [
        TestCollection(name="test1", value=10),
        TestCollection(name="test2", value=20),
        TestCollection(name="test3", value=30),
        TestCollection(name="test4", value=40),
    ]
    await TestCollection.save_all(docs)

    # Test individual conditions first
    results = await TestCollection.find({"value": {"$gt": 20}})
    assert len(results) == 2, "Should find values > 20"
    assert sorted([doc.value for doc in results]) == [30, 40]

    results = await TestCollection.find({"value": {"$lt": 40}})
    assert len(results) == 3, "Should find values < 40"
    assert sorted([doc.value for doc in results]) == [10, 20, 30]

    # Test $and operator with debugging
    filter_dict = {"$and": [{"value": {"$gt": 20}}, {"value": {"$lt": 40}}]}
    print("\nTesting $and operator with filter:", filter_dict)
    results = await TestCollection.find(filter_dict)
    assert len(results) == 1, "Should find one value between 20 and 40"
    assert results[0].value == 30, "Should find value 30"

    # Test $or operator
    results = await TestCollection.find({"$or": [{"value": {"$lt": 15}}, {"value": {"$gt": 35}}]})
    assert len(results) == 2
    assert sorted([doc.value for doc in results]) == [10, 40]

    # Test combined logical operators
    results = await TestCollection.find({"$and": [{"value": {"$gt": 15}}, {"$or": [{"name": "test2"}, {"name": "test3"}]}]})
    assert len(results) == 2
    assert sorted([doc.value for doc in results]) == [20, 30]


@pytest.mark.asyncio
async def test_match_logical_operator():
    """Test the match_logical_operator function directly"""
    from uodm.file_motor_filtering import match_logical_operator, match_condition
    
    # Test document
    doc = {"value": 30, "name": "test3"}
    
    # Test $and operator
    and_conditions = [{"value": {"$gt": 20}}, {"value": {"$lt": 40}}]
    assert match_logical_operator(doc, "$and", and_conditions, match_condition) is True
    
    # Test $or operator
    or_conditions = [{"value": {"$lt": 20}}, {"value": {"$gt": 25}}]
    assert match_logical_operator(doc, "$or", or_conditions, match_condition) is True
    
    # Test failing conditions
    failing_and = [{"value": {"$gt": 40}}, {"value": {"$lt": 50}}]
    assert match_logical_operator(doc, "$and", failing_and, match_condition) is False
    
    failing_or = [{"value": {"$lt": 20}}, {"value": {"$gt": 40}}]
    assert match_logical_operator(doc, "$or", failing_or, match_condition) is False


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
