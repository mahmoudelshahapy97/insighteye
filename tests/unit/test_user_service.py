
import pytest
from unittest.mock import MagicMock, patch
from uuid import uuid4
from app.services.user_service import user_manager

@pytest.fixture
def user_service(mock_db_manager):
    # Patch the db_manager instance attribute on the singleton user_manager
    # This is necessary because user_manager is instantiated at module level
    with patch.object(user_manager, 'db_manager', mock_db_manager):
        yield user_manager

@pytest.mark.asyncio
async def test_create_user(user_service, mock_db_manager):
    # Setup
    user_data = {
        'username': 'testuser',
        'email': 'test@example.com',
        'role': 'user',
        'password': 'Password123!', # Added required password
        'count_of_camera': 5
    }
    mock_db_manager.execute_query.return_value = [{'user_id': uuid4(), **user_data}]
    
    # Execute
    result = await user_service.create_user(**user_data)
    
    # Verify
    assert result is True
    assert mock_db_manager.execute_query.called

@pytest.mark.asyncio
async def test_get_user_by_email(user_service, mock_db_manager):
    email = 'test@example.com'
    mock_db_manager.execute_query.return_value = {'email': email, 'username': 'test'}
    
    user = await user_service.get_user_by_email(email)
    
    assert user is not None
    assert user['email'] == email

