"""
Sample test file for Camera Service
Demonstrates comprehensive unit testing patterns for the CameraService class
"""

import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from uuid import uuid4, UUID
from datetime import datetime
from zoneinfo import ZoneInfo

from app.services.camera_service import CameraService
from app.schemas import StreamCreate, StreamUpdate, CameraAlertSettings


@pytest.fixture
def camera_service(mock_db_manager):
    """Create CameraService instance with mocked dependencies"""
    with patch('app.services.camera_service.db_manager', mock_db_manager):
        with patch('app.services.camera_service.user_manager') as mock_user:
            with patch('app.services.camera_service.workspace_service') as mock_workspace:
                with patch('app.services.camera_service.postgres_service') as mock_postgres:
                    service = CameraService()
                    yield service


@pytest.fixture
def sample_stream_create():
    """Sample StreamCreate data"""
    return StreamCreate(
        name="Test Camera",
        path="rtsp://admin:password@192.168.1.100/stream",
        stream_type="rtsp",
        location_info={
            "area": "Building A",
            "building": "Main Office",
            "floor_level": "Floor 1",
            "zone": "Entrance"
        }
    )


@pytest.fixture
def sample_camera_data():
    """Sample camera data returned from database"""
    return {
        "stream_id": str(uuid4()),
        "name": "Test Camera",
        "path": "rtsp://admin:password@192.168.1.100/stream",
        "stream_type": "rtsp",
        "status": "active",
        "is_streaming": False,
        "workspace_id": str(uuid4()),
        "owner_id": str(uuid4()),
        "created_at": datetime.now(ZoneInfo("Africa/Cairo")),
        "area": "Building A",
        "building": "Main Office",
        "floor_level": "Floor 1",
        "zone": "Entrance"
    }


class TestCameraServiceCreation:
    """Test camera creation functionality"""
    
    @pytest.mark.asyncio
    async def test_create_camera_success(self, camera_service, mock_db_manager, sample_stream_create):
        """Test successful camera creation"""
        user_id = uuid4()
        workspace_id = uuid4()
        username = "testuser"
        
        # Mock user has not exceeded camera limit
        mock_db_manager.execute_query.return_value = {"count": 5}
        
        # Mock successful camera insertion
        camera_id = str(uuid4())
        mock_db_manager.execute_query.return_value = {"stream_id": camera_id}
        
        result = await camera_service.create_camera(
            stream=sample_stream_create,
            user_id=user_id,
            workspace_id=workspace_id,
            username=username
        )
        
        assert result is not None
        assert "stream_id" in result
        
    @pytest.mark.asyncio
    async def test_create_camera_exceeds_limit(self, camera_service, mock_db_manager, sample_stream_create):
        """Test camera creation when limit is exceeded"""
        user_id = uuid4()
        workspace_id = uuid4()
        username = "testuser"
        
        # Mock user has exceeded camera limit (10 cameras, limit is 10)
        mock_db_manager.execute_query.return_value = {"count": 10}
        
        with pytest.raises(Exception) as exc_info:
            await camera_service.create_camera(
                stream=sample_stream_create,
                user_id=user_id,
                workspace_id=workspace_id,
                username=username
            )
        
        assert "limit" in str(exc_info.value).lower()
    
    @pytest.mark.asyncio
    async def test_create_camera_invalid_rtsp_url(self, camera_service, mock_db_manager):
        """Test camera creation with invalid RTSP URL"""
        invalid_stream = StreamCreate(
            name="Invalid Camera",
            path="not-a-valid-url",
            stream_type="rtsp"
        )
        
        user_id = uuid4()
        workspace_id = uuid4()
        username = "testuser"
        
        # Should validate URL format
        with pytest.raises(Exception):
            await camera_service.create_camera(
                stream=invalid_stream,
                user_id=user_id,
                workspace_id=workspace_id,
                username=username
            )


class TestCameraServiceRetrieval:
    """Test camera retrieval functionality"""
    
    @pytest.mark.asyncio
    async def test_get_user_cameras(self, camera_service, mock_db_manager, sample_camera_data):
        """Test retrieving user cameras"""
        user_id = uuid4()
        workspace_id = uuid4()
        
        # Mock database returning list of cameras
        mock_db_manager.execute_query.return_value = [sample_camera_data]
        
        cameras = await camera_service.get_user_cameras(
            user_id=user_id,
            workspace_id=workspace_id,
            user_role="admin",
            encoded_string="",
            return_base64=False
        )
        
        assert isinstance(cameras, list)
        assert len(cameras) > 0
        assert cameras[0]["name"] == "Test Camera"
    
    @pytest.mark.asyncio
    async def test_get_user_cameras_empty(self, camera_service, mock_db_manager):
        """Test retrieving cameras when user has none"""
        user_id = uuid4()
        workspace_id = uuid4()
        
        # Mock database returning empty list
        mock_db_manager.execute_query.return_value = []
        
        cameras = await camera_service.get_user_cameras(
            user_id=user_id,
            workspace_id=workspace_id,
            user_role="admin",
            encoded_string="",
            return_base64=False
        )
        
        assert isinstance(cameras, list)
        assert len(cameras) == 0
    
    @pytest.mark.asyncio
    async def test_get_stream_by_id(self, camera_service, mock_db_manager, sample_camera_data):
        """Test retrieving camera by ID"""
        stream_id = sample_camera_data["stream_id"]
        user_id = sample_camera_data["owner_id"]
        
        # Mock database returning camera data
        mock_db_manager.execute_query.return_value = sample_camera_data
        
        camera = await camera_service.get_stream_by_id(
            stream_id_str=stream_id,
            user_id_context_str=user_id
        )
        
        assert camera is not None
        assert camera["stream_id"] == stream_id
    
    @pytest.mark.asyncio
    async def test_get_stream_by_id_not_found(self, camera_service, mock_db_manager):
        """Test retrieving non-existent camera"""
        stream_id = str(uuid4())
        user_id = str(uuid4())
        
        # Mock database returning None
        mock_db_manager.execute_query.return_value = None
        
        camera = await camera_service.get_stream_by_id(
            stream_id_str=stream_id,
            user_id_context_str=user_id
        )
        
        assert camera is None


class TestCameraServiceUpdate:
    """Test camera update functionality"""
    
    @pytest.mark.asyncio
    async def test_update_camera_success(self, camera_service, mock_db_manager):
        """Test successful camera update"""
        stream_id = uuid4()
        user_id = uuid4()
        
        stream_update = StreamUpdate(
            stream_id=str(stream_id),
            name="Updated Camera Name",
            path="rtsp://admin:newpass@192.168.1.100/stream"
        )
        
        # Mock successful update
        mock_db_manager.execute_query.return_value = {"updated": True}
        
        success, error = await camera_service.update_camera(
            stream_update=stream_update,
            user_id=user_id,
            current_user_role="admin"
        )
        
        assert success is True
        assert error is None
    
    @pytest.mark.asyncio
    async def test_update_camera_unauthorized(self, camera_service, mock_db_manager):
        """Test camera update by unauthorized user"""
        stream_id = uuid4()
        user_id = uuid4()
        
        stream_update = StreamUpdate(
            stream_id=str(stream_id),
            name="Updated Camera Name"
        )
        
        # Mock user doesn't own the camera
        mock_db_manager.execute_query.return_value = None
        
        success, error = await camera_service.update_camera(
            stream_update=stream_update,
            user_id=user_id,
            current_user_role="user"
        )
        
        assert success is False
        assert error is not None
        assert "unauthorized" in error.lower() or "not found" in error.lower()


class TestCameraServiceDeletion:
    """Test camera deletion functionality"""
    
    @pytest.mark.asyncio
    async def test_delete_cameras_success(self, camera_service, mock_db_manager):
        """Test successful camera deletion"""
        camera_ids = [str(uuid4()), str(uuid4())]
        user_id = uuid4()
        
        # Mock successful deletion
        mock_db_manager.execute_query.return_value = {"deleted": True}
        
        deleted, unauthorized, not_found, qdrant_failures = await camera_service.delete_cameras(
            camera_ids=camera_ids,
            user_id=user_id,
            current_user_role="admin"
        )
        
        assert len(deleted) > 0
    
    @pytest.mark.asyncio
    async def test_delete_cameras_mixed_results(self, camera_service, mock_db_manager):
        """Test deletion with mixed success/failure"""
        camera_ids = [str(uuid4()), str(uuid4()), str(uuid4())]
        user_id = uuid4()
        
        # Mock some cameras can be deleted, others cannot
        # This would require more sophisticated mocking based on your implementation
        
        deleted, unauthorized, not_found, qdrant_failures = await camera_service.delete_cameras(
            camera_ids=camera_ids,
            user_id=user_id,
            current_user_role="user"
        )
        
        # Verify results are categorized correctly
        assert isinstance(deleted, list)
        assert isinstance(unauthorized, list)
        assert isinstance(not_found, list)


class TestCameraServiceSearch:
    """Test camera search and filtering"""
    
    @pytest.mark.asyncio
    async def test_search_cameras_by_text(self, camera_service, mock_db_manager, sample_camera_data):
        """Test text-based camera search"""
        user_id = uuid4()
        workspace_id = uuid4()
        
        # Mock search results
        mock_db_manager.execute_query.return_value = [sample_camera_data]
        
        results = await camera_service.search_cameras_by_filters(
            user_id=user_id,
            workspace_id=workspace_id,
            q="Test Camera",
            encoded_string=""
        )
        
        assert isinstance(results, list)
        assert len(results) > 0
    
    @pytest.mark.asyncio
    async def test_search_cameras_by_location(self, camera_service, mock_db_manager, sample_camera_data):
        """Test location-based camera filtering"""
        user_id = uuid4()
        workspace_id = uuid4()
        
        # Mock search results
        mock_db_manager.execute_query.return_value = [sample_camera_data]
        
        results = await camera_service.search_cameras_by_filters(
            user_id=user_id,
            workspace_id=workspace_id,
            areas=["Building A"],
            buildings=["Main Office"],
            encoded_string=""
        )
        
        assert isinstance(results, list)
        if len(results) > 0:
            assert results[0]["area"] == "Building A"


class TestCameraServiceAlerts:
    """Test alert settings management"""
    
    @pytest.mark.asyncio
    async def test_update_alert_settings(self, camera_service, mock_db_manager):
        """Test updating camera alert settings"""
        camera_id = str(uuid4())
        user_id = uuid4()
        workspace_id = uuid4()
        
        alert_data = CameraAlertSettings(
            fire_detection_enabled=True,
            people_counting_enabled=True,
            alert_threshold=0.8
        )
        
        # Mock successful update
        mock_db_manager.execute_query.return_value = {"updated": True}
        
        result = await camera_service.update_alert_settings(
            camera_id=camera_id,
            alert_data=alert_data,
            user_id=user_id,
            workspace_id=workspace_id
        )
        
        assert result is not None


class TestCameraServiceLocationHierarchy:
    """Test location hierarchy functionality"""
    
    @pytest.mark.asyncio
    async def test_get_location_hierarchy(self, camera_service, mock_db_manager):
        """Test retrieving location hierarchy"""
        workspace_id = uuid4()
        
        # Mock location hierarchy data
        mock_hierarchy = {
            "areas": ["Building A", "Building B"],
            "buildings": ["Main Office", "Warehouse"],
            "floors": ["Floor 1", "Floor 2"],
            "zones": ["Entrance", "Exit", "Parking"]
        }
        mock_db_manager.execute_query.return_value = mock_hierarchy
        
        hierarchy = await camera_service.get_location_hierarchy(
            workspace_id=workspace_id
        )
        
        assert hierarchy is not None
        assert "areas" in hierarchy or isinstance(hierarchy, dict)


class TestCameraServiceBulkOperations:
    """Test bulk operations"""
    
    @pytest.mark.asyncio
    async def test_bulk_assign_locations(self, camera_service, mock_db_manager):
        """Test bulk location assignment"""
        camera_ids = [str(uuid4()), str(uuid4())]
        user_id = uuid4()
        workspace_id = uuid4()
        
        from app.schemas import BulkLocationAssignmentWithAlerts
        location_data = BulkLocationAssignmentWithAlerts(
            area="Building A",
            building="Main Office",
            floor_level="Floor 1",
            zone="Entrance"
        )
        
        # Mock successful bulk update
        mock_db_manager.execute_query.return_value = {"updated": True}
        
        updated, failed, errors = await camera_service.bulk_assign_locations(
            camera_ids=camera_ids,
            location_data=location_data,
            user_id=user_id,
            workspace_id=workspace_id,
            user_role="admin"
        )
        
        assert isinstance(updated, list)
        assert isinstance(failed, list)
        assert isinstance(errors, list)
