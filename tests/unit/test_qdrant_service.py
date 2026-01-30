"""
Unit tests for Qdrant Service
Tests valid methods: create_collection, delete_collection, get_collection_count
"""

import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from uuid import uuid4
from app.schemas import SearchQuery

@pytest.fixture
def qdrant_service(mock_qdrant):
    """Create QdrantService instance with mocked client"""
    # The service imports QdrantClient from qdrant_client
    with patch('app.services.qdrant_service.QdrantClient') as mock_client_cls:
        # Mock client instance
        mock_client_instance = MagicMock()
        mock_client_cls.return_value = mock_client_instance
        
        from app.services.qdrant_service import QdrantService
        service = QdrantService()
        
        # Force our mock into the service
        service.qdrant_client = mock_client_instance
        service.get_client = MagicMock(return_value=mock_client_instance)
        
        yield service

@pytest.mark.asyncio
async def test_create_collection_success(qdrant_service):
    """Test creating a collection when it doesn't exist"""
    client = qdrant_service.get_client()
    
    # Simulate get_collection raising unexpected exception (e.g. not found) 
    # OR simpler: check logic. Service: try get_collection, if succeeds return "exists", else create.
    # QdrantClient get_collection raises UnexpectedResponse or similar if not found? 
    # Or maybe it just returns info.
    # The code says:
    # try: client.get_collection(...) return exists
    # except Exception: pass -> create
    
    client.get_collection.side_effect = Exception("Not found")
    client.create_collection.return_value = True
    
    result = await qdrant_service.create_collection("test_col", 128, "COSINE")
    
    assert result["status"] == "success"
    client.create_collection.assert_called_once()

@pytest.mark.asyncio
async def test_create_collection_exists(qdrant_service):
    """Test creating a collection that already exists"""
    client = qdrant_service.get_client()
    
    client.get_collection.return_value = MagicMock(status="green")
    
    result = await qdrant_service.create_collection("test_col", 128, "COSINE")
    
    assert result["status"] == "info"
    client.create_collection.assert_not_called()

@pytest.mark.asyncio
async def test_delete_collection(qdrant_service):
    """Test deleting a collection"""
    client = qdrant_service.get_client()
    
    # Service calls get_collection first
    client.get_collection.return_value = True
    client.delete_collection.return_value = True
    
    result = await qdrant_service.delete_collection("test_col")
    
    assert result["status"] == "success"
    client.delete_collection.assert_called_with(collection_name="test_col")

@pytest.mark.asyncio
async def test_get_collection_count(qdrant_service):
    """Test counting points in collection"""
    client = qdrant_service.get_client()
    
    # Service calls client.count
    client.count.return_value = MagicMock(count=42)
    
    result = await qdrant_service.get_collection_count(
        collection_name="test_col",
        search_query=None,
        user_system_role="admin",
        user_workspace_role=None,
        requesting_username="admin"
    )
    
    assert result["count"] == 42
    client.count.assert_called()
