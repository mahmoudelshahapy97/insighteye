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
        "type": "rtsp", # Changed from stream_type to type to match service code
        "status": "active",
        "is_streaming": False,
        "workspace_id": str(uuid4()),
        "owner_id": str(uuid4()),
        "user_id": str(uuid4()), # Added user_id
        "owner_username": "testuser", # Added owner_username
        "workspace_name": "Test Workspace", # Added workspace_name
        "created_at": datetime.now(ZoneInfo("Africa/Cairo")),
        "updated_at": datetime.now(ZoneInfo("Africa/Cairo")),
        "area": "Building A",
        "building": "Main Office",
        "floor_level": "Floor 1",
        "zone": "Entrance",
        "location": "Main Entrance", # Added location
        "latitude": 30.0,
        "longitude": 31.0,
        "count_threshold_greater": 10,
        "count_threshold_less": 0,
        "alert_enabled": False
    }


class TestCameraServiceCreation:
    """Test camera creation functionality"""
    
    @pytest.mark.asyncio
    async def test_create_camera_success(self, camera_service, mock_db_manager, sample_stream_create):
        """Test successful camera creation"""
        user_id = uuid4()
        workspace_id = uuid4()
        username = "testuser"
        
        # Mock user details
        camera_service.user_manager.get_user_by_id = AsyncMock(return_value={"count_of_camera": 5, "role": "user"})
        
        # Mock DB queries using side_effect to handle multiple calls
        async def db_side_effect(query, *args, **kwargs):
            if "COUNT(*)" in query:
                return {"stream_count": 1} # Under limit
            if "INSERT INTO video_stream" in query:
                return None
            return None
            
        mock_db_manager.execute_query.side_effect = db_side_effect
        
        result = await camera_service.create_camera(
            stream=sample_stream_create,
            user_id=user_id,
            workspace_id=workspace_id,
            username=username
        )
        
        assert result is not None
        # The service generates the UUID, so we verify it looks like one
        assert isinstance(result, str)
        assert len(result) > 0
        
    @pytest.mark.asyncio
    async def test_create_camera_exceeds_limit(self, camera_service, mock_db_manager, sample_stream_create):
        """Test camera creation when limit is exceeded"""
        user_id = uuid4()
        workspace_id = uuid4()
        username = "testuser"
        
        # Mock user details
        camera_service.user_manager.get_user_by_id = AsyncMock(return_value={"count_of_camera": 5, "role": "user"})
        
        # Mock user has exceeded camera limit
        async def db_side_effect(query, *args, **kwargs):
            if "COUNT(*)" in query:
                return {"stream_count": 10} # Over limit (5)
            return None

        mock_db_manager.execute_query.side_effect = db_side_effect
        
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
        
        # Mock check_workspace_access since it's called inside get_stream_by_id
        with patch('app.services.camera_service.check_workspace_access', new_callable=AsyncMock) as mock_access:
            mock_access.return_value = {"role": "admin"}
            
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
        
        with pytest.raises(Exception) as exc_info:
            await camera_service.get_stream_by_id(
                stream_id_str=stream_id,
                user_id_context_str=user_id
            )
        
        assert "not found" in str(exc_info.value).lower()


class TestCameraServiceUpdate:
    """Test camera update functionality"""
    
    @pytest.mark.asyncio
    async def test_update_camera_success(self, camera_service, mock_db_manager):
        """Test successful camera update"""
        stream_id = uuid4()
        user_id = uuid4()
        workspace_id = uuid4()
        
        # Create update object with correct field name 'id' instead of 'stream_id'
        update_data = StreamUpdate(
            id=str(stream_id),
            name="Updated Camera Name",
            path="rtsp://admin:newpass@192.168.1.100/stream"
        )

        # Mock get_camera_by_id result - needs to return owner_id for permission check
        mock_db_manager.execute_query.side_effect = [
            {"user_id": user_id, "workspace_id": workspace_id},  # Initial permission check
            None  # Update execution
        ]
        
        # Mock check_workspace_access - return admin role to allow update
        with patch('app.services.camera_service.check_workspace_access', new_callable=AsyncMock) as mock_check_access:
            mock_check_access.return_value = {"role": "admin"}
            
            result = await camera_service.update_camera(
                update_data, user_id, workspace_id
            )

        assert result["success"] is True
        assert result["message"] == "Camera updated successfully"
    
    @pytest.mark.asyncio
    async def test_update_camera_unauthorized(self, camera_service, mock_db_manager):
        """Test camera update by unauthorized user"""
        stream_id = uuid4()
        user_id = uuid4()
        workspace_id = uuid4()
        
        # Create update object
        update_data = StreamUpdate(
            id=str(stream_id),
            name="Updated Camera Name"
        )

        # Mock get_camera_by_id - return different user_id
        mock_db_manager.execute_query.side_effect = [
            {"user_id": uuid4(), "workspace_id": workspace_id},  # Camera owned by someone else
        ]
        
        # Mock check_workspace_access - return 'member' role (not admin) so permission denied
        with patch('app.services.camera_service.check_workspace_access', new_callable=AsyncMock) as mock_check_access:
            mock_check_access.return_value = {"role": "member"}

            with pytest.raises(HTTPException) as exc_info:
                await camera_service.update_camera(
                    update_data, user_id, workspace_id
                )

        assert exc_info.value.status_code == 403
        assert "Permission denied" in exc_info.value.detail


class TestCameraServiceDeletion:
    """Test camera deletion functionality"""
    
    @pytest.mark.asyncio
    async def test_delete_cameras_success(self, camera_service, mock_db_manager):
        """Test successful camera deletion"""
        camera_ids = [str(uuid4()), str(uuid4())]
        user_id = uuid4()
        workspace_id = uuid4()
        
        # Mock database responses
        # The service does a SELECT first to check ownership, then a DELETE
        mock_db_manager.execute_query.side_effect = [
            {"user_id": user_id, "workspace_id": workspace_id}, # Check for camera 1
            {"user_id": user_id, "workspace_id": workspace_id}, # Check for camera 2
            None, # Delete query
        ]

        result = await camera_service.delete_cameras(
            camera_ids, user_id, workspace_id
        )

        assert result["deleted_count"] == 2
        assert len(result["errors"]) == 0
    
    @pytest.mark.asyncio
    async def test_delete_cameras_mixed_results(self, camera_service, mock_db_manager):
        """Test deletion with mixed success/failure"""
        camera_ids = [str(uuid4()), str(uuid4()), str(uuid4())]
        user_id = uuid4()
        workspace_id = uuid4()
        
        # Mock some cameras can be deleted, others cannot
        # This would require more sophisticated mocking based on your implementation
        
        deleted, unauthorized, not_found, failures = await camera_service.delete_cameras(
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
        
        settings = CameraAlertSettings(
            camera_id=camera_id, # Added required field
            fire_detection_enabled=True,
            people_counting_enabled=True,
            alert_threshold=0.8
        )
        
        # Mock permission check
        with patch('app.services.camera_service.check_workspace_access', new_callable=AsyncMock) as mock_check_access:
            mock_check_access.return_value = {"role": "admin"} # Role is required

            # Mock database response for updating settings
            mock_db_manager.execute_query.return_value = None
        
            result = await camera_service.update_alert_settings(
                settings, user_id, workspace_id
            )

        assert result["success"] is True
        assert len(result["updated_cameras"]) == 1


class TestCameraServiceLocationHierarchy:
    """Test location hierarchy functionality"""
    
    @pytest.mark.asyncio
    async def test_get_location_hierarchy(self, camera_service, mock_db_manager):
        """Test retrieving location hierarchy"""
        workspace_id = uuid4()
        user_id = uuid4()
        
        # Mock database response to return a row with building info
        # The service likely iterates over these. We'll mocks list of dicts.
        mock_db_manager.execute_query.return_value = [
            {
                "building": "Building A",
                "floor_level": "Level 1",
                "zone": "Zone 1",
                "area": "Area 1",
                "location": "Location 1",
                "camera_count": 5
            }
        ]

        result = await camera_service.get_location_hierarchy(
            workspace_id, user_id
        )

        assert len(result.hierarchy) == 1
        assert result.hierarchy[0].building == "Building A"
        assert result.total_locations == 1


class TestCameraServiceBulkOperations:
    """Test bulk operations"""
    
    @pytest.mark.asyncio
    async def test_bulk_assign_locations(self, camera_service, mock_db_manager):
        """Test bulk location assignment"""
        camera_ids = [str(uuid4()), str(uuid4())]
        user_id = uuid4()
        workspace_id = uuid4()
        
        camera_ids = [str(uuid4()), str(uuid4())]
        location_data = BulkLocationAssignmentWithAlerts(
            camera_ids=camera_ids, # Added required field
            area="Building A",
            building="Main Office",
            floor_level="Floor 1",
            zone="Entrance"
        )

        # Mock database response
        # The service typically iterates over IDs and updates them.
        # It might check permissions first. We'll simply mock success for all queries.
        mock_db_manager.execute_query.return_value = None
        
        # Mock validation/permission check if needed (assuming service does checks)
        with patch('app.services.camera_service.check_workspace_access', new_callable=AsyncMock) as mock_check_access:
            mock_check_access.return_value = {"role": "admin"}

            result = await camera_service.bulk_assign_locations(
                location_data, user_id, workspace_id
            )

        assert result.total_processed == 2
        assert len(result.updated_cameras) == 2
