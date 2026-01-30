
import pytest
from unittest.mock import MagicMock, patch, AsyncMock
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
    
    async def db_side_effect(query, *args, **kwargs):
        if "SELECT workspace_id FROM workspaces WHERE name" in query:
            return None # Workspace check -> Not found
        if "INSERT INTO workspaces" in query:
            return None # Insert workspace
        if "INSERT INTO workspace_members" in query:
            return None # Insert member
        return None
        
    mock_db_manager.execute_query.side_effect = db_side_effect
    
    # Mock transaction context manager
    # It needs to return an object that acts as the connection (or just the mock manager itself if that's how it's used)
    # The service uses: async with self.db_manager.transaction() as conn:
    mock_transaction = AsyncMock()
    mock_transaction.__aenter__.return_value = MagicMock()
    mock_transaction.__aexit__.return_value = None
    mock_db_manager.transaction.return_value = mock_transaction
    
    result = await workspace_service.create_workspace(name, description, owner_id)
    
    assert result['name'] == name
    # Should call create workspace AND add member as admin
    assert mock_db_manager.execute_query.call_count >= 1

@pytest.mark.asyncio
async def test_add_workspace_member(workspace_service, mock_db_manager):
    workspace_id = uuid4()
    user_id = uuid4()
    current_user_id = uuid4()
    admin_username = "admin"
    role = 'member'
    
    from app.schemas import WorkspaceMemberCreate
    member_data = WorkspaceMemberCreate(user_id=user_id, role=role)
    
    # Mock user existence check
    with patch('app.services.user_service.user_manager.get_user_by_id', new_callable=AsyncMock) as mock_get_user:
        mock_get_user.return_value = {"username": "testuser"}
        
        mock_db_manager.execute_query.return_value = {
            'workspace_id': workspace_id,
            'user_id': user_id,
            'role': role
        }
        
        # Use patch for the transaction context manager as well since it's used in add_workspace_member
        mock_transaction = AsyncMock()
        mock_transaction.__aenter__.return_value = MagicMock()
        mock_db_manager.transaction.return_value = mock_transaction
        
        result = await workspace_service.add_workspace_member(
            workspace_id, 
            member_data, 
            current_user_id, 
            admin_username, 
            is_admin=True
        )
        
        assert result['role'] == role
