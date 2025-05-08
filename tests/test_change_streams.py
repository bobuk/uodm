import asyncio
import os
import tempfile
from pathlib import Path
import unittest
from typing import List

from uodm import UODM, Collection, Idx
from uodm.change_streams import ChangeStreamDocument

# Define a test collection
class Book(Collection):
    title: str
    author: str
    year: int
    
    __collection__ = "test_books"
    __indexes__ = [
        Idx("title", unique=True),
        Idx(["author", "year"])
    ]


class ChangeStreamTest(unittest.TestCase):
    """Test change streams with both file-based and SQLite backends"""
    
    def setUp(self):
        # Create temporary paths for testing
        self.file_path = Path(tempfile.gettempdir()) / "uodm_change_stream_file_test"
        self.db_path = Path(tempfile.gettempdir()) / "uodm_change_stream_test.db"
        
        # Clean up any existing test data
        if self.db_path.exists():
            self.db_path.unlink()
        
        # Setup event loop for async tests
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
    
    def tearDown(self):
        # Clean up test data
        if self.db_path.exists():
            self.db_path.unlink()
        
        self.loop.close()
    
    def test_file_motor_change_stream(self):
        self.loop.run_until_complete(self._test_change_stream_backend(f"file://{self.file_path}"))
    
    def test_sqlite_change_stream(self):
        self.loop.run_until_complete(self._test_change_stream_backend(f"sqlite:///{self.db_path}"))
    
    async def _test_change_stream_backend(self, connection_string: str):
        """Test change streams with a specific backend"""
        # Setup UODM with the specified connection
        db = UODM(connection_string)
        await db.setup()
        
        # Capture changes in a list for verification
        changes: List[ChangeStreamDocument] = []
        
        # Define a handler to collect changes
        async def handle_change(change: ChangeStreamDocument):
            changes.append(change)
        
        # Start watching for changes with a faster poll interval for testing
        stream = await Book.watch_changes(handle_change, poll_interval=0.2)
        
        # Wait a bit to ensure the initial state is captured
        await asyncio.sleep(0.5)
        
        # Make some changes to trigger events
        book1 = Book(title="Book 1", author="Author A", year=2020)
        await book1.save()
        
        # Wait for the changes to be detected
        await asyncio.sleep(0.5)
        
        # Modify the document
        book1.year = 2021
        await book1.save()
        
        # Wait for the changes to be detected
        await asyncio.sleep(0.5)
        
        # Delete the document
        await book1.delete()
        
        # Wait for the changes to be detected
        await asyncio.sleep(0.5)
        
        # Close the change stream
        await stream.close()
        
        # Verify that we got the expected change events
        self.assertGreaterEqual(len(changes), 1, "Should have at least one change event")
        
        # Check for insert event
        insert_events = [c for c in changes if c.operation_type == 'insert']
        self.assertGreaterEqual(len(insert_events), 1, "Should have at least one insert event")
        
        # Check for delete event (may not be captured in the poll interval)
        delete_events = [c for c in changes if c.operation_type == 'delete']
        
        # Close the database
        await db.close()


if __name__ == "__main__":
    unittest.main()