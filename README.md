# uodm: Ultraminimalistic Object-Document Mapper for async mongodb

Welcome to `uodm`, the fun-sized, feature-packed async MongoDB Object-Document Mapper that will have your documents dancing around faster than you can say "NoSQL nirvana"! 🕺💾

## Features

- 🐍 **Pythonic to the core:** Designed with the grace and grit of modern Python.
- 📚 **Asynchronous bliss:** Leveraging `asyncio` for non-blocking database adventures.
- 🎳 **Simple and expressive models:** Define your models and let uodm handle the rest.
- 🛠️ **Auto-index management:** Because you've got better things to do than managing indexes manually.
- 🌍 **Global database access:** Connect once, use everywhere (kinda like your Netflix subscription but for databases).

## Quick Start

Here's how you can jump into the action:

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

async def main():
    db = uodm.UODM("mongodb://localhost:27017/test")
    await db.setup()
    w = Books(title="War and Peace", author="Tolstoy", year=1869)
    try:
        await w.save()
    except uodm.DuplicateKeyError as e:
        print("this book is already exists")

    res = await Books.get(title="War and Peace")
    print(res)
    for book in await Books.find(year={"$gt": 1900}):
        print(book)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())

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
