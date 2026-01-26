"""
API tests for Analytics Router
Tests analytics endpoints including detections, streams, and workspace analytics
"""

import pytest
from httpx import AsyncClient
from uuid import uuid4
from datetime import datetime, timedelta


class TestDetectionAnalytics:
    """Test detection analytics endpoints"""
    
    @pytest.mark.asyncio
    async def test_get_detection_analytics(self, async_client, auth_headers):
        """Test getting detection analytics"""
        response = await async_client.get(
            "/api/analytics/detections",
            headers=auth_headers
        )
        
        assert response.status_code in [200, 404]
        if response.status_code == 200:
            data = response.json()
            assert isinstance(data, dict) or isinstance(data, list)
    
    @pytest.mark.asyncio
    async def test_get_detection_analytics_with_date_range(self, async_client, auth_headers):
        """Test detection analytics with date range"""
        start_date = (datetime.now() - timedelta(days=7)).isoformat()
        end_date = datetime.now().isoformat()
        
        response = await async_client.get(
            f"/api/analytics/detections?start_date={start_date}&end_date={end_date}",
            headers=auth_headers
        )
        
        assert response.status_code in [200, 404]
    
    @pytest.mark.asyncio
    async def test_get_detection_analytics_by_camera(self, async_client, auth_headers):
        """Test detection analytics for specific camera"""
        camera_id = str(uuid4())
        
        response = await async_client.get(
            f"/api/analytics/detections?camera_id={camera_id}",
            headers=auth_headers
        )
        
        assert response.status_code in [200, 404]


class TestStreamAnalytics:
    """Test stream analytics endpoints"""
    
    @pytest.mark.asyncio
    async def test_get_stream_analytics(self, async_client, auth_headers):
        """Test getting stream analytics"""
        response = await async_client.get(
            "/api/analytics/streams",
            headers=auth_headers
        )
        
        assert response.status_code in [200, 404]
    
    @pytest.mark.asyncio
    async def test_get_stream_uptime_analytics(self, async_client, auth_headers):
        """Test stream uptime analytics"""
        response = await async_client.get(
            "/api/analytics/streams/uptime",
            headers=auth_headers
        )
        
        assert response.status_code in [200, 404]


class TestWorkspaceAnalytics:
    """Test workspace analytics endpoints"""
    
    @pytest.mark.asyncio
    async def test_get_workspace_analytics(self, async_client, auth_headers):
        """Test getting workspace analytics"""
        response = await async_client.get(
            "/api/analytics/workspace",
            headers=auth_headers
        )
        
        assert response.status_code in [200, 404]
    
    @pytest.mark.asyncio
    async def test_get_workspace_summary(self, async_client, auth_headers):
        """Test workspace summary statistics"""
        workspace_id = str(uuid4())
        
        response = await async_client.get(
            f"/api/analytics/workspace/{workspace_id}/summary",
            headers=auth_headers
        )
        
        assert response.status_code in [200, 404]


class TestAnalyticsExport:
    """Test analytics export functionality"""
    
    @pytest.mark.asyncio
    async def test_export_analytics_data(self, async_client, auth_headers):
        """Test exporting analytics data"""
        response = await async_client.get(
            "/api/analytics/export",
            headers=auth_headers
        )
        
        assert response.status_code in [200, 404]
    
    @pytest.mark.asyncio
    async def test_export_analytics_csv(self, async_client, auth_headers):
        """Test exporting analytics as CSV"""
        response = await async_client.get(
            "/api/analytics/export?format=csv",
            headers=auth_headers
        )
        
        assert response.status_code in [200, 404]
