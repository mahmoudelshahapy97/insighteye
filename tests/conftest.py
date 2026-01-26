
import pytest
import asyncio
import os
import sys

# Add app to path
sys.path.append(os.getcwd())

@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture
def mock_db_manager(mocker):
    """Mock the database manager"""
    manager = mocker.AsyncMock()
    # verify connection returns true
    manager.is_connected = True
    return manager
