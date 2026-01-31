
import pytest
import asyncio
import os
import sys
from uuid import uuid4
from httpx import AsyncClient
from datetime import datetime
from zoneinfo import ZoneInfo
from app.services.database import init_db_pool, close_db_pool

os.environ['DB_HOST'] = 'test_db_host'
os.environ['POSTGRES_DB'] = 'test_database'

# Add app to path
sys.path.append(os.getcwd())

@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for async tests"""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture
def mock_db_manager(mocker):
    """Mock the database manager"""
    manager = mocker.AsyncMock()
    # verify connection returns true
    manager.is_connected = True
    manager.execute_query = mocker.AsyncMock()
    manager.fetch_one = mocker.AsyncMock()
    manager.fetch_all = mocker.AsyncMock()
    return manager

@pytest.fixture(scope="session", autouse=True)
async def setup_database():
    """Initialize database pool before all tests and close after."""
    await init_db_pool()
    yield
    await close_db_pool()

@pytest.fixture
async def async_client():
    """HTTP client for API testing"""
    from app.main import app
    from httpx import ASGITransport
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://localhost:8000") as client: #https://te-s.xyz/insighteye
        yield client

@pytest.fixture
def test_user():
    """Create test user data"""
    return {
        "user_id": str(uuid4()),
        "username": "testuser",
        "email": "test@example.com",
        "workspace_id": str(uuid4()),
        "role": "admin",
        "system_role": "admin"
    }

@pytest.fixture
def test_workspace():
    """Create test workspace data"""
    return {
        "workspace_id": str(uuid4()),
        "name": "Test Workspace",
        "max_cameras": 10,
        "max_concurrent_streams": 5,
        "created_at": datetime.now(ZoneInfo("Africa/Cairo"))
    }

@pytest.fixture
def auth_headers(test_user, mocker):
    """Generate auth headers with valid token"""
    # Mock the session manager to create a valid token
    with mocker.patch('app.services.session_service.SessionManager') as mock_session:
        from app.services.session_service import session_manager
        token_pair = session_manager.create_token_pair(
            test_user["user_id"], 
            test_user["workspace_id"]
        )
        return {"Authorization": f"Bearer {token_pair.access_token}"}

@pytest.fixture
def mock_redis(mocker):
    """Mock Redis client"""
    redis_mock = mocker.AsyncMock()
    redis_mock.get = mocker.AsyncMock(return_value=None)
    redis_mock.set = mocker.AsyncMock(return_value=True)
    redis_mock.delete = mocker.AsyncMock(return_value=True)
    return redis_mock

@pytest.fixture
def mock_qdrant(mocker):
    """Mock Qdrant client"""
    qdrant_mock = mocker.AsyncMock()
    qdrant_mock.search = mocker.AsyncMock(return_value=[])
    qdrant_mock.upsert = mocker.AsyncMock(return_value=True)
    qdrant_mock.delete = mocker.AsyncMock(return_value=True)
    return qdrant_mock

@pytest.fixture
def mock_elasticsearch(mocker):
    """Mock Elasticsearch client"""
    es_mock = mocker.AsyncMock()
    es_mock.search = mocker.AsyncMock(return_value={"hits": {"hits": []}})
    es_mock.index = mocker.AsyncMock(return_value={"result": "created"})
    return es_mock

# Configure pytest-asyncio
def pytest_configure(config):
    """Configure pytest with custom markers"""
    config.addinivalue_line(
        "markers", "asyncio: mark test as async"
    )
    config.addinivalue_line(
        "markers", "integration: mark test as integration test"
    )
    config.addinivalue_line(
        "markers", "e2e: mark test as end-to-end test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )
