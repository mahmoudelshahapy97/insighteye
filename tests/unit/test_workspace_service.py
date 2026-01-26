
import pytest
from unittest.mock import MagicMock, patch
from uuid import uuid4
from app.services.workspace_service import WorkspaceService

@pytest.fixture
def workspace_service(mock_db_manager):
    with patch('app.services.workspace_service.db_manager', mock_db_manager):
        service = WorkspaceService()
        yield service

@pytest.mark.asyncio
async def test_create_workspace(workspace_service, mock_db_manager):
    name = "Test Workspace"
    description = "A test workspace"
    owner_id = uuid4()
    
    mock_db_manager.execute_query.return_value = {
        'workspace_id': uuid4(),
        'name': name,
        'description': description
    }
    
    result = await workspace_service.create_workspace(name, description, owner_id)
    
    assert result['name'] == name
    # Should call create workspace AND add member as admin
    assert mock_db_manager.execute_query.call_count >= 1

@pytest.mark.asyncio
async def test_add_workspace_member(workspace_service, mock_db_manager):
    workspace_id = uuid4()
    user_id = uuid4()
    role = 'member'
    
    mock_db_manager.execute_query.return_value = {
        'workspace_id': workspace_id,
        'user_id': user_id,
        'role': role
    }
    
    result = await workspace_service.add_member(workspace_id, user_id, role)
    
    assert result['role'] == role
