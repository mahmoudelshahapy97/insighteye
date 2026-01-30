"""
Unit tests for Elasticsearch Service
Tests valid methods: create_index, delete_index, search_workspace_data, get_camera_ids_for_workspace
"""

import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from uuid import uuid4
from datetime import datetime
from app.schemas import SearchQuery

@pytest.fixture
def elasticsearch_service(mock_elasticsearch):
    """Create ElasticsearchService instance with mocked client"""
    with patch('app.services.elasticsearch_service.AsyncElasticsearch') as mock_es_cls:
        # The service calls AsyncElasticsearch(...) to get the client
        # or it might use a singleton pattern. The code says `client = self.get_client()`
        # We need to see how get_client() is implemented or patch it.
        # Assuming get_client returns self.client if set or creates new.
        
        from app.services.elasticsearch_service import ElasticsearchService
        service = ElasticsearchService()
        
        # Mock the db_manager inside the service
        service.db_manager = AsyncMock()
        
        # Mock the client
        service.client = mock_elasticsearch
        # Patch get_client to return our mock
        service.get_client = MagicMock(return_value=mock_elasticsearch)
        
        yield service

@pytest.mark.asyncio
async def test_create_index(elasticsearch_service, mock_elasticsearch):
    """Test creating an index"""
    index_name = "test_index"
    
    # Mock indices.exists (async)
    mock_elasticsearch.indices.exists = AsyncMock(return_value=False)
    # Mock indices.create (async)
    mock_elasticsearch.indices.create = AsyncMock(return_value={"acknowledged": True})
    
    result = await elasticsearch_service.create_index(index_name)
    
    assert result["status"] == "success"
    mock_elasticsearch.indices.create.assert_called_once()

@pytest.mark.asyncio
async def test_delete_index(elasticsearch_service, mock_elasticsearch):
    """Test deleting an index"""
    index_name = "test_index"
    
    mock_elasticsearch.indices.exists = AsyncMock(return_value=True)
    mock_elasticsearch.indices.delete = AsyncMock(return_value={"acknowledged": True})
    
    result = await elasticsearch_service.delete_index(index_name)
    
    assert result["status"] == "success"
    mock_elasticsearch.indices.delete.assert_called_once()

@pytest.mark.asyncio
async def test_search_workspace_data(elasticsearch_service, mock_elasticsearch):
    """Test search_workspace_data logic"""
    workspace_id = uuid4()
    search_query = SearchQuery(start_date="2024-01-01")
    
    # Mock index existence check
    mock_elasticsearch.indices.exists = AsyncMock(return_value=True)
    
    # Mock count
    mock_elasticsearch.count = AsyncMock(return_value={"count": 5})
    
    # Mock search
    mock_hits = {
        "hits": {
            "hits": [
                {
                    "_source": {
                        "timestamp": 1700000000,
                        "camera_id": "cam1",
                        "person_count": 2
                    }
                }
            ]
        }
    }
    mock_elasticsearch.search = AsyncMock(return_value=mock_hits)
    
    result = await elasticsearch_service.search_workspace_data(
        workspace_id=workspace_id,
        search_query=search_query,
        user_system_role="admin",
        user_workspace_role="owner",
        requesting_username="admin"
    )
    
    assert result["total_count"] == 5
    assert len(result["data"]) == 1
    mock_elasticsearch.search.assert_called()

@pytest.mark.asyncio
async def test_get_camera_ids_for_workspace(elasticsearch_service):
    """Test getting camera IDs from DB"""
    workspace_id = uuid4()
    
    # Mock DB response
    elasticsearch_service.db_manager.execute_query.return_value = [
        {"stream_id": uuid4(), "name": "Cam 1"},
        {"stream_id": uuid4(), "name": "Cam 2"}
    ]
    
    cameras = await elasticsearch_service.get_camera_ids_for_workspace(workspace_id)
    
    assert len(cameras) == 2
    assert "id" in cameras[0]
    assert "name" in cameras[0]
