import asyncio
import json
import aiosqlite
from bson import ObjectId

async def test_sqlite_json():
    print("\nSQLite JSON Query Test")
    print("======================")
    
    # Connect to an in-memory SQLite database
    conn = await aiosqlite.connect(":memory:")
    
    # Create a test table
    await conn.execute("""
        CREATE TABLE test_docs (
            _id TEXT PRIMARY KEY,
            doc BLOB NOT NULL
        )
    """)
    
    # Insert test documents
    books = [
        {"_id": str(ObjectId()), "title": "War and Peace", "author": "Leo Tolstoy", "year": 1869},
        {"_id": str(ObjectId()), "title": "1984", "author": "George Orwell", "year": 1949},
        {"_id": str(ObjectId()), "title": "To Kill a Mockingbird", "author": "Harper Lee", "year": 1960},
    ]
    
    for book in books:
        await conn.execute(
            'INSERT INTO test_docs (_id, doc) VALUES (?, ?)',
            (book["_id"], json.dumps(book).encode())
        )
    
    await conn.commit()
    
    # Debug: Show all documents
    cursor = await conn.execute('SELECT _id, doc FROM test_docs')
    rows = await cursor.fetchall()
    print(f"\nStored documents ({len(list(rows))}):")
    for _id, doc in rows:
        book = json.loads(doc)
        print(f"- {book['title']} by {book['author']} ({book['year']})")
    
    # Test: Query by author (exact match)
    print("\nTest 1: Query by author (George Orwell)")
    
    # Debug: Print actual values in the database
    cursor = await conn.execute('SELECT json_extract(doc, "$.author") FROM test_docs')
    rows = await cursor.fetchall()
    print("Author values in database:")
    for (author,) in rows:
        print(f"- '{author}' (type: {type(author).__name__})")
    
    # Try different ways to query
    print("\nTrying different query approaches:")
    
    # Approach 1: Direct comparison
    cursor = await conn.execute(
        'SELECT doc FROM test_docs WHERE json_extract(doc, "$.author") = ?',
        ("George Orwell",)
    )
    rows = await cursor.fetchall()
    print(f"Approach 1 - Direct comparison: {len(list(rows))} results")
    for doc, in rows:
        book = json.loads(doc)
        print(f"- {book['title']} by {book['author']} ({book['year']})")
    
    # Approach 2: Using json() function
    cursor = await conn.execute(
        'SELECT doc FROM test_docs WHERE json_extract(doc, "$.author") = json(?)',
        (json.dumps("George Orwell"),)
    )
    rows = await cursor.fetchall()
    print(f"Approach 2 - Using json(): {len(list(rows))} results")
    for doc, in rows:
        book = json.loads(doc)
        print(f"- {book['title']} by {book['author']} ({book['year']})")
    
    # Approach 3: Direct string comparison
    cursor = await conn.execute(
        'SELECT doc FROM test_docs WHERE json_extract(doc, "$.author") = "George Orwell"'
    )
    rows = await cursor.fetchall()
    print(f"Approach 3 - Direct string in SQL: {len(list(rows))} results")
    for doc, in rows:
        book = json.loads(doc)
        print(f"- {book['title']} by {book['author']} ({book['year']})")
    
    # Test: Query by year with $gt
    print("\nTest 2: Query books after 1950")
    cursor = await conn.execute(
        'SELECT doc FROM test_docs WHERE CAST(json_extract(doc, "$.year") AS INTEGER) > 1950'
    )
    rows = await cursor.fetchall()
    print(f"Result count: {len(list(rows))}")
    for doc, in rows:
        book = json.loads(doc)
        print(f"- {book['title']} by {book['author']} ({book['year']})")
    
    # Close connection
    await conn.close()

if __name__ == "__main__":
    asyncio.run(test_sqlite_json())
