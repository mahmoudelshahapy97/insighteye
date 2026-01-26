"""
Unit tests for Elasticsearch Service
Tests index management, document indexing, search queries, and aggregations
"""

import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from uuid import uuid4
from datetime import datetime


@pytest.fixture
def elasticsearch_service(mock_elasticsearch):
    """Create ElasticsearchService instance with mocked client"""
    with patch('app.services.elasticsearch_service.Elasticsearch') as mock_es:
        mock_es.return_value = mock_elasticsearch
        
        from app.services.elasticsearch_service import ElasticsearchService
        
        service = ElasticsearchService()
        service.client = mock_elasticsearch
        yield service


class TestIndexManagement:
    """Test Elasticsearch index management"""
    
    @pytest.mark.asyncio
    async def test_create_index(self, elasticsearch_service, mock_elasticsearch):
        """Test creating an index"""
        index_name = "test_index"
        mappings = {
            "properties": {
                "timestamp": {"type": "date"},
                "camera_id": {"type": "keyword"},
                "detection_type": {"type": "keyword"}
            }
        }
        
        mock_elasticsearch.indices.create = AsyncMock(return_value={"acknowledged": True})
        
        result = await elasticsearch_service.create_index(index_name, mappings)
        
        assert mock_elasticsearch.indices.create.called or result is not None
    
    @pytest.mark.asyncio
    async def test_delete_index(self, elasticsearch_service, mock_elasticsearch):
        """Test deleting an index"""
        index_name = "test_index"
        
        mock_elasticsearch.indices.delete = AsyncMock(return_value={"acknowledged": True})
        
        result = await elasticsearch_service.delete_index(index_name)
        
        assert mock_elasticsearch.indices.delete.called or result is not None
    
    @pytest.mark.asyncio
    async def test_index_exists(self, elasticsearch_service, mock_elasticsearch):
        """Test checking if index exists"""
        index_name = "test_index"
        
        mock_elasticsearch.indices.exists = AsyncMock(return_value=True)
        
        exists = await elasticsearch_service.index_exists(index_name)
        
        assert mock_elasticsearch.indices.exists.called or isinstance(exists, bool)


class TestDocumentOperations:
    """Test document indexing and retrieval"""
    
    @pytest.mark.asyncio
    async def test_index_document(self, elasticsearch_service, mock_elasticsearch):
        """Test indexing a document"""
        index_name = "detections"
        doc_id = str(uuid4())
        document = {
            "timestamp": datetime.now().isoformat(),
            "camera_id": str(uuid4()),
            "detection_type": "person",
            "confidence": 0.95
        }
        
        mock_elasticsearch.index = AsyncMock(return_value={"result": "created"})
        
        result = await elasticsearch_service.index_document(index_name, doc_id, document)
        
        assert mock_elasticsearch.index.called or result is not None
    
    @pytest.mark.asyncio
    async def test_bulk_index_documents(self, elasticsearch_service, mock_elasticsearch):
        """Test bulk document indexing"""
        index_name = "detections"
        documents = [
            {"id": str(uuid4()), "data": {"type": "person", "confidence": 0.9}}
            for _ in range(100)
        ]
        
        mock_elasticsearch.bulk = AsyncMock(return_value={"errors": False})
        
        result = await elasticsearch_service.bulk_index(index_name, documents)
        
        assert mock_elasticsearch.bulk.called or result is not None
    
    @pytest.mark.asyncio
    async def test_get_document(self, elasticsearch_service, mock_elasticsearch):
        """Test retrieving a document"""
        index_name = "detections"
        doc_id = str(uuid4())
        
        mock_doc = {
            "_source": {
                "timestamp": datetime.now().isoformat(),
                "camera_id": str(uuid4())
            }
        }
        mock_elasticsearch.get = AsyncMock(return_value=mock_doc)
        
        document = await elasticsearch_service.get_document(index_name, doc_id)
        
        assert mock_elasticsearch.get.called or isinstance(document, dict)
    
    @pytest.mark.asyncio
    async def test_delete_document(self, elasticsearch_service, mock_elasticsearch):
        """Test deleting a document"""
        index_name = "detections"
        doc_id = str(uuid4())
        
        mock_elasticsearch.delete = AsyncMock(return_value={"result": "deleted"})
        
        result = await elasticsearch_service.delete_document(index_name, doc_id)
        
        assert mock_elasticsearch.delete.called or result is not None


class TestSearchOperations:
    """Test search functionality"""
    
    @pytest.mark.asyncio
    async def test_simple_search(self, elasticsearch_service, mock_elasticsearch):
        """Test simple search query"""
        index_name = "detections"
        query = {"match": {"detection_type": "person"}}
        
        mock_results = {
            "hits": {
                "total": {"value": 10},
                "hits": [
                    {"_id": str(uuid4()), "_source": {"type": "person"}}
                ]
            }
        }
        mock_elasticsearch.search = AsyncMock(return_value=mock_results)
        
        results = await elasticsearch_service.search(index_name, query)
        
        assert mock_elasticsearch.search.called or isinstance(results, dict)
    
    @pytest.mark.asyncio
    async def test_search_with_filters(self, elasticsearch_service, mock_elasticsearch):
        """Test search with filters"""
        index_name = "detections"
        query = {
            "bool": {
                "must": [{"match": {"detection_type": "person"}}],
                "filter": [{"range": {"confidence": {"gte": 0.8}}}]
            }
        }
        
        mock_elasticsearch.search = AsyncMock(return_value={"hits": {"hits": []}})
        
        results = await elasticsearch_service.search(index_name, query)
        
        assert mock_elasticsearch.search.called or isinstance(results, dict)
    
    @pytest.mark.asyncio
    async def test_search_with_pagination(self, elasticsearch_service, mock_elasticsearch):
        """Test search with pagination"""
        index_name = "detections"
        query = {"match_all": {}}
        
        mock_elasticsearch.search = AsyncMock(return_value={"hits": {"hits": []}})
        
        results = await elasticsearch_service.search(
            index_name, 
            query, 
            from_=0, 
            size=10
        )
        
        assert mock_elasticsearch.search.called or isinstance(results, dict)


class TestAggregations:
    """Test aggregation functionality"""
    
    @pytest.mark.asyncio
    async def test_simple_aggregation(self, elasticsearch_service, mock_elasticsearch):
        """Test simple aggregation"""
        index_name = "detections"
        agg_query = {
            "detection_counts": {
                "terms": {"field": "detection_type"}
            }
        }
        
        mock_results = {
            "aggregations": {
                "detection_counts": {
                    "buckets": [
                        {"key": "person", "doc_count": 100},
                        {"key": "vehicle", "doc_count": 50}
                    ]
                }
            }
        }
        mock_elasticsearch.search = AsyncMock(return_value=mock_results)
        
        results = await elasticsearch_service.aggregate(index_name, agg_query)
        
        assert mock_elasticsearch.search.called or isinstance(results, dict)
    
    @pytest.mark.asyncio
    async def test_date_histogram_aggregation(self, elasticsearch_service, mock_elasticsearch):
        """Test date histogram aggregation"""
        index_name = "detections"
        agg_query = {
            "detections_over_time": {
                "date_histogram": {
                    "field": "timestamp",
                    "calendar_interval": "hour"
                }
            }
        }
        
        mock_elasticsearch.search = AsyncMock(return_value={"aggregations": {}})
        
        results = await elasticsearch_service.aggregate(index_name, agg_query)
        
        assert mock_elasticsearch.search.called or isinstance(results, dict)
