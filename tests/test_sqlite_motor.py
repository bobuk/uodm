import asyncio
import os
import tempfile
from pathlib import Path
import unittest

from uodm import UODM, Collection, Idx

class Book(Collection):
    title: str
    author: str
    year: int
    
    __collection__ = "test_books"
    __indexes__ = [
        Idx("title", unique=True),
        Idx(["author", "year"])
    ]

class SQLiteMotorTest(unittest.TestCase):
    def setUp(self):
        # Create a temporary SQLite database for testing
        self.db_path = Path(tempfile.gettempdir()) / "uodm_test.db"
        
        # Make sure it doesn't exist from a previous test
        if self.db_path.exists():
            self.db_path.unlink()
        
        # Setup event loop for async tests
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        
        # Setup UODM with SQLite connection
        self.db = UODM(f"sqlite:///{self.db_path}")
        self.loop.run_until_complete(self.db.setup())

    def tearDown(self):
        # Clean up the database connection
        self.loop.run_until_complete(self.db.close())
        
        # Remove test database
        if self.db_path.exists():
            self.db_path.unlink()
        
        self.loop.close()

    def test_crud_operations(self):
        async def run_test():
            # Create
            book = Book(title="Test Book", author="Test Author", year=2020)
            await book.save()
            self.assertIsNotNone(book._id)
            
            # Read
            retrieved = await Book.get(title="Test Book")
            self.assertIsNotNone(retrieved)
            if retrieved:  # Type check to avoid None issues
                self.assertEqual(retrieved.title, "Test Book")
                self.assertEqual(retrieved.author, "Test Author")
                self.assertEqual(retrieved.year, 2020)
                
                # Update
                retrieved.year = 2021
                await retrieved.save()
                updated = await Book.get(title="Test Book")
                if updated:  # Type check
                    self.assertEqual(updated.year, 2021)
                    
                    # Delete
                    await updated.delete()
            deleted = await Book.get(title="Test Book")
            self.assertIsNone(deleted)
        
        self.loop.run_until_complete(run_test())

    def test_query_operations(self):
        async def run_test():
            # Insert test data
            books = [
                Book(title="Book 1", author="Author A", year=2000),
                Book(title="Book 2", author="Author B", year=2005),
                Book(title="Book 3", author="Author A", year=2010),
                Book(title="Book 4", author="Author C", year=2015),
                Book(title="Book 5", author="Author B", year=2020),
            ]
            for book in books:
                await book.save()
            
            # Test basic find
            results = await Book.find(author="Author A")
            self.assertEqual(len(results), 2)
            
            # Test operators
            results = await Book.find(year={"$gt": 2010})
            self.assertEqual(len(results), 2)
            
            # Test sorting (ascending)
            results = await Book.find(sort="year")
            self.assertEqual(results[0].title, "Book 1")
            
            # Test sorting (descending)
            results = await Book.find(sort="-year")
            self.assertEqual(results[0].title, "Book 5")
            
            # Test pagination
            results = await Book.find(sort="year", skip=1, limit=2)
            self.assertEqual(len(results), 2)
            self.assertEqual(results[0].title, "Book 2")
            
            # Test count
            count = await Book.count(author="Author B")
            self.assertEqual(count, 2)
            
            # Test bulk update
            books = await Book.find(author="Author A")
            await Book.update(books, year=3000)
            updated = await Book.find(year=3000)
            self.assertEqual(len(updated), 2)
        
        self.loop.run_until_complete(run_test())

    def test_in_memory_database(self):
        async def run_test():
            # Create an in-memory database
            mem_db = UODM("sqlite://")
            await mem_db.setup()
            
            # Save a document
            book = Book(title="Memory Test", author="Test", year=2020)
            await book.save()
            
            # Verify it exists
            retrieved = await Book.get(title="Memory Test")
            self.assertIsNotNone(retrieved)
            
            # Close the connection
            await mem_db.close()
        
        self.loop.run_until_complete(run_test())


if __name__ == "__main__":
    unittest.main()
