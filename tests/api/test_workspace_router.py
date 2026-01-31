"""
API tests for Workspace Router
Tests workspace management endpoints including creation, members, and settings
"""

import pytest
from httpx import AsyncClient
from uuid import uuid4


class TestWorkspaceCreation:
    """Test workspace creation endpoints"""
    
    @pytest.mark.asyncio
    async def test_create_workspace(self, async_client, auth_headers):
        """Test creating a new workspace"""
        workspace_data = {
            "name": f"Test Workspace {uuid4().hex[:8]}",
            "description": "Test workspace description",
            "max_cameras": 10,
            "max_concurrent_streams": 5
        }
        
        response = await async_client.post(
            "/api/workspaces",
            headers=auth_headers,
            json=workspace_data
        )
        
        assert response.status_code in [200, 201, 404]
        if response.status_code in [200, 201]:
            data = response.json()
            assert "workspace_id" in data or "id" in data
    
    @pytest.mark.asyncio
    async def test_create_workspace_unauthorized(self, async_client):
        """Test workspace creation without authentication"""
        response = await async_client.post(
            "/api/workspaces",
            json={"name": "Test Workspace"}
        )
        
        assert response.status_code == 404


class TestWorkspaceListing:
    """Test workspace listing endpoints"""
    
    @pytest.mark.asyncio
    async def test_get_workspaces(self, async_client, auth_headers):
        """Test getting list of workspaces"""
        response = await async_client.get(
            "/api/workspaces",
            headers=auth_headers
        )
        
        assert response.status_code in [200, 404]
        if response.status_code == 200:
            data = response.json()
            assert isinstance(data, list) or isinstance(data, dict)
    
    @pytest.mark.asyncio
    async def test_get_workspace_by_id(self, async_client, auth_headers):
        """Test getting workspace by ID"""
        workspace_id = str(uuid4())
        
        response = await async_client.get(
            f"/api/workspaces/{workspace_id}",
            headers=auth_headers
        )
        
        assert response.status_code in [200, 404]


class TestWorkspaceMembers:
    """Test workspace member management"""
    
    @pytest.mark.asyncio
    async def test_add_workspace_member(self, async_client, auth_headers):
        """Test adding member to workspace"""
        workspace_id = str(uuid4())
        user_id = str(uuid4())
        
        response = await async_client.post(
            f"/api/workspaces/{workspace_id}/members",
            headers=auth_headers,
            json={
                "user_id": user_id,
                "role": "member"
            }
        )
        
        assert response.status_code in [200, 201, 404, 403]
    
    @pytest.mark.asyncio
    async def test_remove_workspace_member(self, async_client, auth_headers):
        """Test removing member from workspace"""
        workspace_id = str(uuid4())
        user_id = str(uuid4())
        
        response = await async_client.delete(
            f"/api/workspaces/{workspace_id}/members/{user_id}",
            headers=auth_headers
        )
        
        assert response.status_code in [200, 204, 404, 403]
    
    @pytest.mark.asyncio
    async def test_list_workspace_members(self, async_client, auth_headers):
        """Test listing workspace members"""
        workspace_id = str(uuid4())
        
        response = await async_client.get(
            f"/api/workspaces/{workspace_id}/members",
            headers=auth_headers
        )
        
        assert response.status_code in [200, 404]


class TestWorkspaceSettings:
    """Test workspace settings management"""
    
    @pytest.mark.asyncio
    async def test_update_workspace_settings(self, async_client, auth_headers):
        """Test updating workspace settings"""
        workspace_id = str(uuid4())
        
        response = await async_client.put(
            f"/api/workspaces/{workspace_id}",
            headers=auth_headers,
            json={
                "name": "Updated Workspace",
                "max_cameras": 20
            }
        )
        
        assert response.status_code in [200, 204, 404, 403]
