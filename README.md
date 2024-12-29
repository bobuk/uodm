# uodm: Ultraminimalistic Object-Document Mapper for async mongodb and file storage

Welcome to `uodm`, the fun-sized, feature-packed async Object-Document Mapper that supports both MongoDB and file-based storage! Get your documents dancing around faster than you can say "NoSQL nirvana"! 🕺💾

## Features

- 🐍 **Pythonic to the core:** Designed with the grace and grit of modern Python.
- 📚 **Asynchronous bliss:** Leveraging `asyncio` for non-blocking database operations.
- 🎳 **Simple and expressive models:** Define your models and let uodm handle the rest.
- 🛠️ **Auto-index management:** Because you've got better things to do than managing indexes manually.
- 🌍 **Global database access:** Connect once, use everywhere (kinda like your Netflix subscription but for databases).
- 📁 **File-based storage:** Perfect for development, testing, or simple applications without MongoDB (supported since v0.2).

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
    import asyncio
    asyncio.run(file_example())
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

---
Feel free to connect with me or just stop by to say "Hi, MongoDB!" 👋
