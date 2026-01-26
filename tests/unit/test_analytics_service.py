
import pytest
from unittest.mock import MagicMock, patch
from uuid import uuid4
from datetime import date
from app.services.analytics_service import AnalyticsService

@pytest.fixture
def analytics_service(mock_db_manager):
    with patch('app.services.analytics_service.db_manager', mock_db_manager):
        service = AnalyticsService()
        yield service

@pytest.mark.asyncio
async def test_get_unique_cameras_no_filter(analytics_service, mock_db_manager):
    workspace_id = uuid4()
    
    # Mock result
    mock_db_manager.execute_query.return_value = [
        {
            "stream_id": uuid4(),
            "camera_name": "Cam 1",
            "location": "Loc A",
            "area": "Area 1",
            "building": "Bldg 1",
            "zone": "Zone 1",
            "floor_level": "1"
        }
    ]
    
    result = await analytics_service.get_unique_cameras(workspace_id)
    
    assert result['success'] is True
    assert result['count'] == 1
    
    # Verify query didn't have extra filters
    args, _ = mock_db_manager.execute_query.call_args
    query = args[0]
    assert "WHERE workspace_id = $1" in query
    assert "location IN" not in query

@pytest.mark.asyncio
async def test_get_unique_cameras_with_filter(analytics_service, mock_db_manager):
    workspace_id = uuid4()
    locations = ["Hallway", "Entrance"]
    
    mock_db_manager.execute_query.return_value = []
    
    await analytics_service.get_unique_cameras(workspace_id, locations=locations)
    
    # Verify query HAS location filter
    args, _ = mock_db_manager.execute_query.call_args
    query = args[0]
    assert "location IN ($2, $3)" in query
    
    # Verify params
    params = args[1]
    assert len(params) == 3 # workspace + 2 locations
    assert params[1] == "Hallway"

@pytest.mark.asyncio
async def test_get_frame_counts_per_camera(analytics_service, mock_db_manager):
    workspace_id = uuid4()
    start_date = date(2023, 1, 1)
    
    mock_db_manager.execute_query.return_value = [
        {
            "camera_name": "Cam 1",
            "total_frames": 100
        }
    ]
    
    result = await analytics_service.get_frame_counts_per_camera(
        workspace_id, start_date=start_date
    )
    
    assert result['success'] is True
    assert len(result['data']) == 1
    
    # Verify date filter
    args, _ = mock_db_manager.execute_query.call_args
    query = args[0]
    assert "date >= $2" in query

@pytest.mark.asyncio
async def test_get_average_people_per_camera(analytics_service, mock_db_manager):
    workspace_id = uuid4()
    
    mock_db_manager.execute_query.return_value = [
        {
            "camera_name": "Cam 1",
            "avg_person_count": 5.5
        }
    ]
    
    result = await analytics_service.get_average_people_per_camera(workspace_id)
    
    assert result['success'] is True
    assert result['data'][0]['avg_person_count'] == 6 # Should round 5.5 to 6
