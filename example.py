import sys; sys.path.append('src')
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
    try:
        w = Books(title="War and Peace", author="Tolstoy", year = 1869)
        await w.save()
    except uodm.DuplicateKeyError as e:
        print(e)

    try:
        w2 = Books(title = "World War Z", author = "Max Brooks", year = 2006)
        await w2.save()
    except uodm.DuplicateKeyError as e:
        print(e)

    res = await Books.get(title="War and Peace")
    print(res)
    for book in await Books.find(year = {"$gt": 1900}):
        print(book)






if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
