import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch
from typing import List

from uodm import UODM, Collection
from uodm.change_streams import ChangeStreamDocument, PollingChangeStream


# Define a test collection
class Book(Collection):
    title: str
    author: str
    year: int
    
    __collection__ = "test_books"


class MongoDBFallbackTest(unittest.TestCase):
    """Test MongoDB fallback to polling when replication isn't available"""
    
    def setUp(self):
        # Setup event loop for async tests
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
    
    def tearDown(self):
        self.loop.close()
    
    @patch('motor.motor_asyncio.AsyncIOMotorClient')
    def test_mongodb_without_replication(self, mock_motor_client):
        """Test fallback to polling when MongoDB doesn't have replication enabled"""
        self.loop.run_until_complete(self._test_mongodb_without_replication(mock_motor_client))
    
    async def _test_mongodb_without_replication(self, mock_motor_client):
        # Setup mock MongoDB client
        mock_client = MagicMock()
        mock_motor_client.return_value = mock_client
        
        # Setup mock admin command to simulate MongoDB without replication
        mock_admin = MagicMock()
        mock_client.admin = mock_admin
        mock_command = AsyncMock()
        # Return a response without 'setName' to simulate standalone MongoDB
        mock_command.return_value = {
            "ismaster": True,
            "maxBsonObjectSize": 16777216,
            "maxMessageSizeBytes": 48000000,
            "maxWriteBatchSize": 100000,
            "localTime": "2023-05-08T12:00:00Z",
            "connectionId": 1,
            "ok": 1.0
            # No 'setName' = no replication
        }
        mock_admin.command = mock_command
        
        # Setup mock database and collection
        mock_db = MagicMock()
        mock_client.get_default_database.return_value = mock_db
        mock_collection = MagicMock()
        mock_db.get_collection.return_value = mock_collection
        
        # Connect to mock MongoDB
        db = UODM("mongodb://localhost:27017/test")
        
        # Capture changes in a list for verification
        changes: List[ChangeStreamDocument] = []
        
        # Define a handler to collect changes
        async def handle_change(change: ChangeStreamDocument):
            changes.append(change)
        
        # Start watching for changes
        with patch('uodm.uodm.PollingChangeStream') as mock_polling:
            # Create a spy to verify PollingChangeStream is used
            original_polling = PollingChangeStream
            mock_polling.side_effect = lambda *args, **kwargs: original_polling(*args, **kwargs)
            
            # Watch for changes
            stream = await Book.watch_changes(handle_change)
            
            # Verify the polling implementation was used
            mock_polling.assert_called_once()
            
            # Close the change stream
            await stream.close()
        
        # Verify command was called to check replication status
        mock_command.assert_called_once_with('isMaster')
        
        await db.close()


if __name__ == "__main__":
    unittest.main()