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

```python
async def file_example():
    # Use file-based storage
    db = uodm.UODM("file:///path/to/data")  # Stores data in JSON files
    await db.setup()
    
    # Same API as MongoDB!
    w = Books(title="1984", author="Orwell", year=1949)
    await w.save()
    
    res = await Books.get(title="1984")
    print(res)

if __name__ == "__main__":
    import asyncio
    asyncio.run(mongodb_example())
    # or
    # asyncio.run(file_example())
```

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
