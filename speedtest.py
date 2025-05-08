import asyncio
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import List

from uodm import UODM, Collection
from uodm.types import SerializationFormat


class TestDoc(Collection):
    name: str
    value: int
    created: datetime
    tags: List[str]


async def run_benchmark(format: SerializationFormat):
    # Setup
    db_path = Path("./testdb")
    if db_path.exists():
        shutil.rmtree(db_path)

    print(f"\nStarting UODM File Storage Speed Test ({format.value})")
    print("-" * 50)

    # Initialize DB
    db = UODM(f"file://{db_path}#{format.value}")
    await db.set_db("speedtest")

    # Write Test
    print("\nWrite Test - Inserting 50,000 documents")
    start_time = time.time()

    docs = []
    for i in range(50000):
        doc = TestDoc(name=f"test_{i}", value=i, created=datetime.now(), tags=[f"tag_{i % 5}", f"group_{i % 10}"])
        docs.append(doc)

    await TestDoc.save_all(docs)

    write_time = time.time() - start_time
    print(f"Write completed in {write_time:.2f} seconds")
    print(f"Average write speed: {50000/write_time:.2f} docs/second")

    # Read Tests
    print("\nRead Tests:")

    # Single document read
    start_time = time.time()
    single_doc = await TestDoc.get(name="test_1000")
    if single_doc is not None:
        single_read_time = time.time() - start_time
        print(f"Single document read: {single_read_time:.4f} seconds")

    # Multiple document read with filter
    start_time = time.time()
    results = await TestDoc.find(tags=["tag_1"])
    filter_read_time = time.time() - start_time
    print(f"Filtered read ({len(results)} docs): {filter_read_time:.2f} seconds")

    # Sorted read
    start_time = time.time()
    results = await TestDoc.find(sort="value", limit=1000)
    sort_read_time = time.time() - start_time
    print(f"Sorted read (1000 docs): {sort_read_time:.2f} seconds")

    # Update Test
    print("\nUpdate Test:")
    start_time = time.time()
    update_docs = results[:100]
    await TestDoc.update(update_docs, value=99999)
    update_time = time.time() - start_time
    print(f"Bulk update (100 docs): {update_time:.2f} seconds")

    # Count Test
    start_time = time.time()
    count = await TestDoc.count(tags=["tag_2"])
    count_time = time.time() - start_time
    print(f"\nCount query: {count_time:.2f} seconds")
    print(f"Found {count} documents")

    print("\nMemory Usage:")
    db_size = sum(f.stat().st_size for f in db_path.glob("**/*") if f.is_file())
    print(f"Database size: {db_size/1024/1024:.2f} MB")
    print(f"Average document size: {db_size/50000:.2f} bytes")

    # Cleanup
    if db_path.exists():
        shutil.rmtree(db_path)


async def main():
    # Run with different serialization formats
    await run_benchmark(SerializationFormat.ORJSON)
    await run_benchmark(SerializationFormat.JSON)
    await run_benchmark(SerializationFormat.PICKLE)


if __name__ == "__main__":
    asyncio.run(main())
