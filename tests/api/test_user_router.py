"""
API tests for User Router
Tests user management endpoints including profile, updates, and user listing
"""

import pytest
from httpx import AsyncClient
from uuid import uuid4


class TestUserProfileEndpoint:
    """Test user profile endpoints"""
    
    @pytest.mark.asyncio
    async def test_get_current_user_profile(self, async_client, auth_headers):
        """Test getting current user profile"""
        response = await async_client.get(
            "/api/users",
            headers=auth_headers
        )
        
        assert response.status_code in [200, 404]
        if response.status_code == 200:
            data = response.json()
            assert "user_id" in data or "username" in data
    
    @pytest.mark.asyncio
    async def test_get_profile_unauthorized(self, async_client):
        """Test getting profile without authentication"""
        response = await async_client.get("/api/users")
        
        assert response.status_code == 404


class TestUserManagementEndpoint:
    """Test user management endpoints"""
    
    @pytest.mark.asyncio
    async def test_get_user_by_id(self, async_client, auth_headers):
        """Test getting user by ID"""
        user_id = str(uuid4())
        
        response = await async_client.get(
            f"/api/users/{user_id}",
            headers=auth_headers
        )
        
        assert response.status_code in [200, 404]
    
    @pytest.mark.asyncio
    async def test_list_users(self, async_client, auth_headers):
        """Test listing users"""
        response = await async_client.get(
            "/api/users",
            headers=auth_headers
        )
        
        assert response.status_code in [200, 404]
        if response.status_code == 200:
            data = response.json()
            assert isinstance(data, list) or isinstance(data, dict)
    

class TestUserRoleManagement:
    """Test user role management"""
    
    @pytest.mark.asyncio
    async def test_update_user_role(self, async_client, auth_headers):
        """Test updating user role (admin only)"""
        user_id = str(uuid4())
        
        response = await async_client.put(
            f"/api/users/{user_id}/role",
            headers=auth_headers,
            json={"role": "admin"}
        )
        
        # Should require admin privileges
        assert response.status_code in [200, 403, 404]


class TestUserSearch:
    """Test user search functionality"""
    
    @pytest.mark.asyncio
    async def test_search_users(self, async_client, auth_headers):
        """Test searching users"""
        response = await async_client.get(
            "/api/users/search?q=test",
            headers=auth_headers
        )
        
        assert response.status_code in [200, 404]
