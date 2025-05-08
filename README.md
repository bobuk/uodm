# uodm: Ultraminimalistic Object-Document Mapper for async mongodb and file storage

Welcome to `uodm`, the fun-sized, feature-packed async Object-Document Mapper that supports both MongoDB and file-based storage! Get your documents dancing around faster than you can say "NoSQL nirvana"! 🕺💾

## Features

- 🐍 **Pythonic to the core:** Designed with the grace and grit of modern Python.
- 📚 **Asynchronous bliss:** Leveraging `asyncio` for non-blocking database operations.
- 🎳 **Simple and expressive models:** Define your models and let uodm handle the rest.
- 🛠️ **Auto-index management:** Because you've got better things to do than managing indexes manually.
- 🔄 **Change streams:** Monitor collection changes in real-time with a consistent API across all backends.
- 🌍 **Global database access:** Connect once, use everywhere (kinda like your Netflix subscription but for databases).
- 🗄️ **Multiple storage backends:** Choose the right storage for your needs:
  - **MongoDB:** For production-grade applications with scalability requirements
  - **SQLite:** For embedded applications or when you need more robust local storage
  - **File-based:** For development, testing, or simple applications

## Quick Start

Here's how you can jump into the action:

### MongoDB Storage

```python
import uodm

class Books(uodm.Collection):
    title: str
    author: str
    year: int = uodm.Field(default=2021)

    __collection__ = "mybooks"
    __indexes__ = [
        uodm.Idx("title", unique=True),
        uodm.Idx(["title", "author"], sparse=True),
    ]

async def mongodb_example():
    # Connect to MongoDB
    db = uodm.UODM("mongodb://localhost:27017/test")
    await db.setup()

    w = Books(title="War and Peace", author="Tolstoy", year=1869)
    try:
        await w.save()
    except uodm.DuplicateKeyError as e:
        print("this book already exists")

    res = await Books.get(title="War and Peace")
    print(res)

    books = await Books.find(year={"$gt": 1900})
    for book in books:
        print(book)
```

### SQLite Storage

UODM now supports SQLite as a robust local database option that bridges the gap between file-based storage and MongoDB:

```python
async def sqlite_example():
    # Connect to SQLite database
    # File-based SQLite database
    db = uodm.UODM("sqlite:///path/to/database.db")

    # Or use in-memory SQLite database for testing
    # db = uodm.UODM("sqlite://")

    await db.setup()

    # Same API as MongoDB and file storage!
    w = Books(title="War and Peace", author="Tolstoy", year=1869)
    await w.save()

    # All MongoDB-style queries work
    book = await Books.get(title="War and Peace")
    books = await Books.find(year={"$gt": 1900})

    # Remember to close connections when done (important for SQLite)
    await db.close()
```

SQLite storage is perfect for:
- Embedded applications requiring a local database
- Applications needing better transactional support than file-based storage
- Higher performance with larger datasets than file-based storage
- Development and testing without MongoDB infrastructure

SQLite data is stored in a single database file, with each collection as a separate table. Indexes are stored both as SQLite indexes (for better query performance) and as metadata for compatibility with the ODM layer.

### File-based Storage

UODM supports file-based storage as a drop-in replacement for MongoDB. This is perfect for:
- Development and testing
- Simple applications that don't need a full database
- Offline-first applications
- Data portability scenarios

You can choose from three serialization formats:
- JSON (default) - Human readable, widely compatible
- ORJSON - Faster JSON processing (requires `pip install orjson`)
- Pickle - For Python-specific data types

```python
async def file_example():
    # Basic file storage with JSON
    db = uodm.UODM("file:///path/to/data")

    # Or specify a format:
    db = uodm.UODM("file:///path/to/data#json")  # explicit JSON
    db = uodm.UODM("file:///path/to/data#orjson")  # faster JSON
    db = uodm.UODM("file:///path/to/data#pickle")  # Python pickle format

    await db.setup()

    # Same API as MongoDB!
    w = Books(title="1984", author="Orwell", year=1949)
    await w.save()

    # All MongoDB-style queries work
    res = await Books.get(title="1984")
    books = await Books.find(year={"$gt": 1900})
    count = await Books.count(author="Orwell")

    # Sorting and pagination
    recent = await Books.find(
        sort="-year",  # descending sort by year
        limit=10,      # only 10 results
        skip=20        # skip first 20 matches
    )

if __name__ == "__main__":
    print("\nStarting GT operator test...")
    asyncio.run(test_gt_operator())
    print("Test completed.\n")
```

File storage creates a directory structure that mirrors MongoDB:
```
/path/to/data/
  └── default/           # database name
      └── books/        # collection name
          ├── _indexes.json       # index definitions
          ├── 507f1f77bcf86cd799439011.json  # document
          └── 507f1f77bcf86cd799439012.json  # document
```

Each document is stored in its own JSON file (or pickle/orjson), named by its ID.
Indexes are maintained in a special `_indexes.json` file.

### Change Streams

UODM now supports change streams across all backends, allowing you to monitor and react to changes in your collections in real-time. MongoDB uses native change streams, while SQLite and file-based backends use a polling-based implementation that mimics the same API.

```python
import asyncio
import uodm

class Books(uodm.Collection):
    title: str
    author: str
    year: int

    __collection__ = "books"

async def handle_change(change):
    """React to changes in the books collection"""
    op_type = change.operation_type

    if op_type == 'insert':
        doc = change.full_document
        print(f"New book added: '{doc.get('title')}'")
    elif op_type == 'update':
        doc = change.full_document
        print(f"Book updated: '{doc.get('title')}'")
    elif op_type == 'delete':
        doc_id = change.document_key.get('_id')
        print(f"Book deleted: ID {doc_id}")

async def change_stream_example():
    # Connect to any supported backend
    db = uodm.UODM("file:///path/to/data")
    # Or use MongoDB: db = uodm.UODM("mongodb://localhost:27017/test")
    # Or use SQLite: db = uodm.UODM("sqlite:///path/to/database.db")

    await db.setup()

    # Start watching for changes
    stream = await Books.watch_changes(handle_change)

    # Make some changes
    book = Books(title="War and Peace", author="Tolstoy", year=1869)
    await book.save()

    # Update the book
    book.year = 1870
    await book.save()

    # Delete the book
    await book.delete()

    # When done, close the change stream
    await stream.close()

    # Close the database connection
    await db.close()
```

For MongoDB, this uses native change streams. For file and SQLite backends, UODM automatically falls back to a polling-based implementation that provides the same consistent API.

### Limitations of File-based Storage

While file-based storage is great for development and simple applications, be aware of these limitations:

1. **Performance**
   - No true indexing support (indexes are simulated)
   - Full collection scans for complex queries
   - Slower than MongoDB for large datasets
   - Memory usage increases with collection size during queries

2. **Concurrency**
   - Basic file locking for writes
   - No atomic operations
   - No transactions support
   - Potential race conditions under heavy load

3. **Scalability**
   - Limited by filesystem performance
   - Not suitable for high-concurrency applications
   - Directory size limits (depending on filesystem)
   - All data must fit on a single machine

4. **Query Support**
   - Limited MongoDB query operators supported
   - No aggregation pipeline
   - No geospatial queries
   - No text search capabilities

Best suited for:
- Development and testing
- Single-user applications
- Small to medium datasets (<100K documents)
- Scenarios where MongoDB setup is overkill

## Installation

Grab the latest release with a simple pip command:

``` bash
pip install git+https://github.com/bobuk/uodm.git#egg=uodm
```

or just `pip install uodm` if you're ok to use the latest release.
Don't forget to wear your seatbelt, this thing is fast! 🚗💨


## Contributing

Pull requests? Issues? Existential queries about your database? All are welcome!

1. Fork it (like stealing your neighbor's wifi but legal).
2. Create your feature branch (git checkout -b my-new-feature).
3. Commit your changes (git commit -am 'Add some feature').
4. Push to the branch (git push origin my-new-feature).
5. Create a new Pull Request.
6. ...
7. PROFIT

## License

This project is under "The Unlicense". Yes, it's really unlicensed. Like a fisherman without a fishing license. 🎣
