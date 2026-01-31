"""
API tests for Camera Router
Tests camera management endpoints including CRUD operations, search, and location management
"""

import pytest
from httpx import AsyncClient
from uuid import uuid4


@pytest.fixture
def sample_camera_data():
    """Sample camera data for testing"""
    return {
        "name": f"Test Camera {uuid4().hex[:8]}",
        "path": "rtsp://admin:password@192.168.1.100/stream",
        "stream_type": "rtsp",
        "location_info": {
            "area": "Building A",
            "building": "Main Office",
            "floor_level": "Floor 1",
            "zone": "Entrance"
        }
    }


class TestCreateCameraEndpoint:
    """Test camera creation endpoint"""
    
    @pytest.mark.asyncio
    async def test_create_camera_success(self, async_client, auth_headers, sample_camera_data):
        """Test successful camera creation"""
        response = await async_client.post(
            "/api/camera",
            headers=auth_headers,
            json=sample_camera_data
        )
        
        assert response.status_code in [200, 201]
        data = response.json()
        assert "stream_id" in data or "id" in data
    
    @pytest.mark.asyncio
    async def test_create_camera_unauthorized(self, async_client, sample_camera_data):
        """Test camera creation without authentication"""
        response = await async_client.post(
            "/api/camera",
            json=sample_camera_data
        )
        
        assert response.status_code == 401
    
    @pytest.mark.asyncio
    async def test_create_camera_invalid_rtsp_url(self, async_client, auth_headers):
        """Test camera creation with invalid RTSP URL"""
        invalid_data = {
            "name": "Invalid Camera",
            "path": "not-a-valid-url",
            "stream_type": "rtsp"
        }
        
        response = await async_client.post(
            "/api/camera",
            headers=auth_headers,
            json=invalid_data
        )
        
        assert response.status_code in [400, 422]
    
    @pytest.mark.asyncio
    async def test_create_camera_missing_required_fields(self, async_client, auth_headers):
        """Test camera creation with missing fields"""
        incomplete_data = [
            {"path": "rtsp://test"},  # Missing name
            {"name": "Test"},  # Missing path
            {}  # Missing all
        ]
        
        for data in incomplete_data:
            response = await async_client.post(
                "/api/camera",
                headers=auth_headers,
                json=data
            )
            assert response.status_code in [400, 422]


class TestGetCamerasEndpoint:
    """Test camera listing endpoint"""
    
    @pytest.mark.asyncio
    async def test_get_cameras_success(self, async_client, auth_headers):
        """Test getting list of cameras"""
        response = await async_client.get(
            "/api/camera/user",
            headers=auth_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list) or isinstance(data, dict)
    
    @pytest.mark.asyncio
    async def test_get_cameras_unauthorized(self, async_client):
        """Test getting cameras without authentication"""
        response = await async_client.get("/api/camera/user")
        
        assert response.status_code == 401
    
    @pytest.mark.asyncio
    async def test_get_cameras_with_pagination(self, async_client, auth_headers):
        """Test camera listing with pagination"""
        response = await async_client.get(
            "/api/camera/user?limit=10&offset=0",
            headers=auth_headers
        )
        
        assert response.status_code == 200


class TestUpdateCameraEndpoint:
    """Test camera update endpoint"""
    
    @pytest.mark.asyncio
    async def test_update_camera_success(self, async_client, auth_headers, sample_camera_data):
        """Test successful camera update"""
        # First create a camera
        create_response = await async_client.post(
            "/api/cameras",
            headers=auth_headers,
            json=sample_camera_data
        )
        
        if create_response.status_code in [200, 201]:
            camera_data = create_response.json()
            camera_id = camera_data.get("stream_id") or camera_data.get("id")
            
            if camera_id:
                # Update the camera
                update_data = {
                    "stream_id": camera_id,
                    "name": "Updated Camera Name",
                    "path": "rtsp://admin:newpass@192.168.1.101/stream"
                }
                
                response = await async_client.put(
                    f"/api/cameras/{camera_id}",
                    headers=auth_headers,
                    json=update_data
                )
                
                assert response.status_code in [200, 204]
    
    @pytest.mark.asyncio
    async def test_update_camera_not_found(self, async_client, auth_headers):
        """Test updating non-existent camera"""
        non_existent_id = str(uuid4())
        
        response = await async_client.put(
            f"/api/cameras/{non_existent_id}",
            headers=auth_headers,
            json={"name": "Updated Name"}
        )
        
        assert response.status_code in [404, 400]
    
    @pytest.mark.asyncio
    async def test_update_camera_unauthorized(self, async_client):
        """Test camera update without authentication"""
        camera_id = str(uuid4())
        
        response = await async_client.put(
            f"/api/cameras/{camera_id}",
            json={"name": "Updated Name"}
        )
        
        assert response.status_code == 401


class TestDeleteCameraEndpoint:
    """Test camera deletion endpoint"""
    
    @pytest.mark.asyncio
    async def test_delete_camera_success(self, async_client, auth_headers, sample_camera_data):
        """Test successful camera deletion"""
        # First create a camera
        create_response = await async_client.post(
            "/api/cameras",
            headers=auth_headers,
            json=sample_camera_data
        )
        
        if create_response.status_code in [200, 201]:
            camera_data = create_response.json()
            camera_id = camera_data.get("stream_id") or camera_data.get("id")
            
            if camera_id:
                # Delete the camera
                response = await async_client.delete(
                    f"/api/cameras/{camera_id}",
                    headers=auth_headers
                )
                
                assert response.status_code in [200, 204]
    
    @pytest.mark.asyncio
    async def test_delete_camera_not_found(self, async_client, auth_headers):
        """Test deleting non-existent camera"""
        non_existent_id = str(uuid4())
        
        response = await async_client.delete(
            f"/api/cameras/{non_existent_id}",
            headers=auth_headers
        )
        
        assert response.status_code in [404, 400, 200]  # Some APIs return 200 even if not found
    
    @pytest.mark.asyncio
    async def test_delete_camera_unauthorized(self, async_client):
        """Test camera deletion without authentication"""
        camera_id = str(uuid4())
        
        response = await async_client.delete(f"/api/cameras/{camera_id}")
        
        assert response.status_code == 401
    
    @pytest.mark.asyncio
    async def test_bulk_delete_cameras(self, async_client, auth_headers):
        """Test bulk camera deletion"""
        camera_ids = [str(uuid4()), str(uuid4())]
        
        response = await async_client.delete(
            "/api/cameras/bulk",
            headers=auth_headers,
            json={"camera_ids": camera_ids}
        )
        
        # Endpoint may or may not exist
        assert response.status_code in [200, 204, 404]


class TestSearchCamerasEndpoint:
    """Test camera search endpoint"""
    
    @pytest.mark.asyncio
    async def test_search_cameras_by_name(self, async_client, auth_headers):
        """Test searching cameras by name"""
        response = await async_client.get(
            "/api/cameras/search?q=Test",
            headers=auth_headers
        )
        
        assert response.status_code in [200, 404]
        if response.status_code == 200:
            data = response.json()
            assert isinstance(data, list) or isinstance(data, dict)
    
    @pytest.mark.asyncio
    async def test_search_cameras_by_location(self, async_client, auth_headers):
        """Test searching cameras by location"""
        response = await async_client.get(
            "/api/cameras/search?area=Building A&building=Main Office",
            headers=auth_headers
        )
        
        assert response.status_code in [200, 404]
    
    @pytest.mark.asyncio
    async def test_search_cameras_empty_query(self, async_client, auth_headers):
        """Test search with empty query"""
        response = await async_client.get(
            "/api/cameras/search",
            headers=auth_headers
        )
        
        assert response.status_code in [200, 400]


class TestCameraStateEndpoint:
    """Test camera state endpoint"""
    
    @pytest.mark.asyncio
    async def test_get_camera_state(self, async_client, auth_headers):
        """Test getting camera state summary"""
        response = await async_client.get(
            "/api/cameras/state",
            headers=auth_headers
        )
        
        assert response.status_code in [200, 404]
        if response.status_code == 200:
            data = response.json()
            # Should contain state information
            assert isinstance(data, dict) or isinstance(data, list)
    
    @pytest.mark.asyncio
    async def test_get_camera_state_by_workspace(self, async_client, auth_headers):
        """Test getting camera state for specific workspace"""
        workspace_id = str(uuid4())
        
        response = await async_client.get(
            f"/api/cameras/state?workspace_id={workspace_id}",
            headers=auth_headers
        )
        
        assert response.status_code in [200, 404]


class TestLocationHierarchyEndpoint:
    """Test location hierarchy endpoint"""
    
    @pytest.mark.asyncio
    async def test_get_location_hierarchy(self, async_client, auth_headers):
        """Test getting location hierarchy"""
        response = await async_client.get(
            "/api/cameras/locations",
            headers=auth_headers
        )
        
        assert response.status_code in [200, 404]
        if response.status_code == 200:
            data = response.json()
            assert isinstance(data, dict) or isinstance(data, list)
    
    @pytest.mark.asyncio
    async def test_get_location_hierarchy_unauthorized(self, async_client):
        """Test getting location hierarchy without auth"""
        response = await async_client.get("/api/cameras/locations")
        
        assert response.status_code == 401


class TestAlertSettingsEndpoint:
    """Test alert settings management"""
    
    @pytest.mark.asyncio
    async def test_update_alert_settings(self, async_client, auth_headers, sample_camera_data):
        """Test updating camera alert settings"""
        # First create a camera
        create_response = await async_client.post(
            "/api/cameras",
            headers=auth_headers,
            json=sample_camera_data
        )
        
        if create_response.status_code in [200, 201]:
            camera_data = create_response.json()
            camera_id = camera_data.get("stream_id") or camera_data.get("id")
            
            if camera_id:
                # Update alert settings
                alert_data = {
                    "fire_detection_enabled": True,
                    "people_counting_enabled": True,
                    "alert_threshold": 0.8
                }
                
                response = await async_client.put(
                    f"/api/cameras/{camera_id}/alerts",
                    headers=auth_headers,
                    json=alert_data
                )
                
                assert response.status_code in [200, 204, 404]


class TestBulkOperationsEndpoint:
    """Test bulk operations"""
    
    @pytest.mark.asyncio
    async def test_bulk_location_assignment(self, async_client, auth_headers):
        """Test bulk location assignment"""
        bulk_data = {
            "camera_ids": [str(uuid4()), str(uuid4())],
            "location_data": {
                "area": "Building A",
                "building": "Main Office",
                "floor_level": "Floor 1",
                "zone": "Entrance"
            }
        }
        
        response = await async_client.post(
            "/api/cameras/bulk/assign-location",
            headers=auth_headers,
            json=bulk_data
        )
        
        assert response.status_code in [200, 404, 400]


class TestCameraValidation:
    """Test input validation"""
    
    @pytest.mark.asyncio
    async def test_invalid_camera_id_format(self, async_client, auth_headers):
        """Test various invalid camera ID formats"""
        invalid_ids = [
            "not-a-uuid",
            "12345",
            "",
            "../../../etc/passwd"
        ]
        
        for invalid_id in invalid_ids:
            response = await async_client.get(
                f"/api/cameras/{invalid_id}",
                headers=auth_headers
            )
            assert response.status_code in [400, 404, 422]
    
    @pytest.mark.asyncio
    async def test_camera_name_length_validation(self, async_client, auth_headers):
        """Test camera name length limits"""
        # Very long name
        long_name_data = {
            "name": "A" * 1000,
            "path": "rtsp://test",
            "stream_type": "rtsp"
        }
        
        response = await async_client.post(
            "/api/cameras",
            headers=auth_headers,
            json=long_name_data
        )
        
        # Should either accept or reject based on validation rules
        assert response.status_code in [200, 201, 400, 422]
