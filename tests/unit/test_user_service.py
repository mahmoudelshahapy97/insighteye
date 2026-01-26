
import pytest
from unittest.mock import MagicMock, patch
from uuid import uuid4
from app.services.user_service import UserService

@pytest.fixture
def user_service(mock_db_manager):
    with patch('app.services.user_service.db_manager', mock_db_manager):
        service = UserService()
        yield service

@pytest.mark.asyncio
async def test_create_user(user_service, mock_db_manager):
    # Setup
    user_data = {
        'username': 'testuser',
        'email': 'test@example.com',
        'role': 'user'
    }
    mock_db_manager.execute_query.return_value = [{'user_id': uuid4(), **user_data}]
    
    # Execute
    result = await user_service.create_user(**user_data)
    
    # Verify
    assert result is not None
    assert result['username'] == 'testuser'
    assert mock_db_manager.execute_query.called

@pytest.mark.asyncio
async def test_get_user_by_email(user_service, mock_db_manager):
    email = 'test@example.com'
    mock_db_manager.execute_query.return_value = {'email': email, 'username': 'test'}
    
    user = await user_service.get_user_by_email(email)
    
    assert user is not None
    assert user['email'] == email

@pytest.mark.asyncio
async def test_update_user_profile(user_service, mock_db_manager):
    user_id = uuid4()
    update_data = {'username': 'newname'}
    mock_db_manager.execute_query.return_value = {'user_id': user_id, **update_data}
    
    result = await user_service.update_user(user_id, update_data)
    
    assert result['username'] == 'newname'
