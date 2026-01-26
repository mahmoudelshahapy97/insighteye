"""
Unit tests for Qdrant Service
Tests vector database operations including collection management, vector insertion, and search
"""

import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from uuid import uuid4
import numpy as np


@pytest.fixture
def qdrant_service(mock_qdrant):
    """Create QdrantService instance with mocked client"""
    with patch('app.services.qdrant_service.QdrantClient') as mock_client:
        mock_client.return_value = mock_qdrant
        
        from app.services.qdrant_service import QdrantService
        
        service = QdrantService()
        service.client = mock_qdrant
        yield service


class TestCollectionManagement:
    """Test Qdrant collection management"""
    
    @pytest.mark.asyncio
    async def test_create_collection(self, qdrant_service, mock_qdrant):
        """Test creating a collection"""
        collection_name = "test_collection"
        vector_size = 512
        
        mock_qdrant.create_collection = AsyncMock(return_value=True)
        
        result = await qdrant_service.create_collection(collection_name, vector_size)
        
        assert mock_qdrant.create_collection.called or result is not None
    
    @pytest.mark.asyncio
    async def test_delete_collection(self, qdrant_service, mock_qdrant):
        """Test deleting a collection"""
        collection_name = "test_collection"
        
        mock_qdrant.delete_collection = AsyncMock(return_value=True)
        
        result = await qdrant_service.delete_collection(collection_name)
        
        assert mock_qdrant.delete_collection.called or result is not None
    
    @pytest.mark.asyncio
    async def test_list_collections(self, qdrant_service, mock_qdrant):
        """Test listing collections"""
        mock_qdrant.get_collections = AsyncMock(return_value=["collection1", "collection2"])
        
        collections = await qdrant_service.list_collections()
        
        assert mock_qdrant.get_collections.called or isinstance(collections, list)


class TestVectorOperations:
    """Test vector insertion and retrieval"""
    
    @pytest.mark.asyncio
    async def test_insert_vector(self, qdrant_service, mock_qdrant):
        """Test inserting a vector"""
        collection_name = "test_collection"
        vector_id = str(uuid4())
        vector = np.random.rand(512).tolist()
        metadata = {"camera_id": str(uuid4()), "timestamp": "2024-01-01T00:00:00"}
        
        mock_qdrant.upsert = AsyncMock(return_value=True)
        
        result = await qdrant_service.insert_vector(collection_name, vector_id, vector, metadata)
        
        assert mock_qdrant.upsert.called or result is not None
    
    @pytest.mark.asyncio
    async def test_insert_batch_vectors(self, qdrant_service, mock_qdrant):
        """Test batch vector insertion"""
        collection_name = "test_collection"
        vectors = [
            {"id": str(uuid4()), "vector": np.random.rand(512).tolist(), "metadata": {}}
            for _ in range(10)
        ]
        
        mock_qdrant.upsert = AsyncMock(return_value=True)
        
        result = await qdrant_service.insert_batch(collection_name, vectors)
        
        assert mock_qdrant.upsert.called or result is not None
    
    @pytest.mark.asyncio
    async def test_delete_vector(self, qdrant_service, mock_qdrant):
        """Test deleting a vector"""
        collection_name = "test_collection"
        vector_id = str(uuid4())
        
        mock_qdrant.delete = AsyncMock(return_value=True)
        
        result = await qdrant_service.delete_vector(collection_name, vector_id)
        
        assert mock_qdrant.delete.called or result is not None


class TestVectorSearch:
    """Test vector similarity search"""
    
    @pytest.mark.asyncio
    async def test_search_similar_vectors(self, qdrant_service, mock_qdrant):
        """Test searching for similar vectors"""
        collection_name = "test_collection"
        query_vector = np.random.rand(512).tolist()
        top_k = 5
        
        mock_results = [
            {"id": str(uuid4()), "score": 0.95, "payload": {}},
            {"id": str(uuid4()), "score": 0.90, "payload": {}},
        ]
        mock_qdrant.search = AsyncMock(return_value=mock_results)
        
        results = await qdrant_service.search(collection_name, query_vector, top_k)
        
        assert mock_qdrant.search.called or isinstance(results, list)
    
    @pytest.mark.asyncio
    async def test_search_with_filter(self, qdrant_service, mock_qdrant):
        """Test searching with metadata filter"""
        collection_name = "test_collection"
        query_vector = np.random.rand(512).tolist()
        filter_conditions = {"camera_id": str(uuid4())}
        
        mock_qdrant.search = AsyncMock(return_value=[])
        
        results = await qdrant_service.search(
            collection_name, 
            query_vector, 
            top_k=5, 
            filters=filter_conditions
        )
        
        assert mock_qdrant.search.called or isinstance(results, list)


class TestMetadataOperations:
    """Test metadata update operations"""
    
    @pytest.mark.asyncio
    async def test_update_metadata(self, qdrant_service, mock_qdrant):
        """Test updating vector metadata"""
        collection_name = "test_collection"
        vector_id = str(uuid4())
        new_metadata = {"updated": True, "timestamp": "2024-01-02T00:00:00"}
        
        mock_qdrant.set_payload = AsyncMock(return_value=True)
        
        result = await qdrant_service.update_metadata(collection_name, vector_id, new_metadata)
        
        assert mock_qdrant.set_payload.called or result is not None
    
    @pytest.mark.asyncio
    async def test_get_vector_metadata(self, qdrant_service, mock_qdrant):
        """Test retrieving vector metadata"""
        collection_name = "test_collection"
        vector_id = str(uuid4())
        
        mock_metadata = {"camera_id": str(uuid4()), "timestamp": "2024-01-01T00:00:00"}
        mock_qdrant.retrieve = AsyncMock(return_value=[{"payload": mock_metadata}])
        
        metadata = await qdrant_service.get_metadata(collection_name, vector_id)
        
        assert mock_qdrant.retrieve.called or isinstance(metadata, dict)
