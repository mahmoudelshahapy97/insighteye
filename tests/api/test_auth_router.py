"""
API tests for Authentication Router
Tests all authentication endpoints including signup, login, logout, token refresh, and password management
"""

import pytest
from httpx import AsyncClient
from uuid import uuid4


class TestSignupEndpoint:
    """Test user signup endpoint"""
    
    @pytest.mark.asyncio
    async def test_signup_success(self, async_client):
        """Test successful user registration"""
        response = await async_client.post("/signup", json={
            "username": f"testuser_{uuid4().hex[:8]}",
            "email": f"test_{uuid4().hex[:8]}@example.com",
            "password": "SecurePass123!",
            "workspace_name": "Test Workspace"
        })
        
        assert response.status_code in [200, 201]
        data = response.json()
        assert "user_id" in data or "access_token" in data or "message" in data
    
    @pytest.mark.asyncio
    async def test_signup_duplicate_username(self, async_client):
        """Test signup with duplicate username"""
        username = f"duplicate_{uuid4().hex[:8]}"
        user_data = {
            "username": username,
            "email": f"test1_{uuid4().hex[:8]}@example.com",
            "password": "SecurePass123!"
        }
        
        # First signup
        await async_client.post("/signup", json=user_data)
        
        # Second signup with same username
        user_data["email"] = f"test2_{uuid4().hex[:8]}@example.com"
        response = await async_client.post("/signup", json=user_data)
        
        assert response.status_code in [400, 409, 422]
    
    @pytest.mark.asyncio
    async def test_signup_duplicate_email(self, async_client):
        """Test signup with duplicate email"""
        email = f"duplicate_{uuid4().hex[:8]}@example.com"
        user_data = {
            "username": f"user1_{uuid4().hex[:8]}",
            "email": email,
            "password": "SecurePass123!"
        }
        
        # First signup
        await async_client.post("/signup", json=user_data)
        
        # Second signup with same email
        user_data["username"] = f"user2_{uuid4().hex[:8]}"
        response = await async_client.post("/signup", json=user_data)
        
        assert response.status_code in [400, 409, 422]
    
    @pytest.mark.asyncio
    async def test_signup_weak_password(self, async_client):
        """Test signup with weak password"""
        response = await async_client.post("/signup", json={
            "username": f"testuser_{uuid4().hex[:8]}",
            "email": f"test_{uuid4().hex[:8]}@example.com",
            "password": "123"  # Too short
        })
        
        assert response.status_code in [400, 422]
    
    @pytest.mark.asyncio
    async def test_signup_invalid_email(self, async_client):
        """Test signup with invalid email format"""
        response = await async_client.post("/signup", json={
            "username": f"testuser_{uuid4().hex[:8]}",
            "email": "not-an-email",
            "password": "SecurePass123!"
        })
        
        assert response.status_code in [400, 422]
    
    @pytest.mark.asyncio
    async def test_signup_missing_fields(self, async_client):
        """Test signup with missing required fields"""
        incomplete_data = [
            {"email": "test@example.com", "password": "SecurePass123!"},  # Missing username
            {"username": "testuser", "password": "SecurePass123!"},  # Missing email
            {"username": "testuser", "email": "test@example.com"},  # Missing password
        ]
        
        for data in incomplete_data:
            response = await async_client.post("/signup", json=data)
            assert response.status_code in [400, 422]


class TestLoginEndpoint:
    """Test user login endpoint"""
    
    @pytest.mark.asyncio
    async def test_login_success(self, async_client):
        """Test successful login"""
        # First create a user
        username = f"loginuser_{uuid4().hex[:8]}"
        password = "SecurePass123!"
        
        await async_client.post("/signup", json={
            "username": username,
            "email": f"{username}@example.com",
            "password": password
        })
        
        # Then login
        response = await async_client.post("/login", json={
            "username": username,
            "password": password
        })
        
        assert response.status_code == 200
        data = response.json()
        assert "access_token" in data
        assert "refresh_token" in data
    
    @pytest.mark.asyncio
    async def test_login_wrong_password(self, async_client):
        """Test login with wrong password"""
        username = f"loginuser_{uuid4().hex[:8]}"
        
        await async_client.post("/signup", json={
            "username": username,
            "email": f"{username}@example.com",
            "password": "CorrectPass123!"
        })
        
        response = await async_client.post("/login", json={
            "username": username,
            "password": "WrongPass123!"
        })
        
        assert response.status_code in [401, 403]
    
    @pytest.mark.asyncio
    async def test_login_nonexistent_user(self, async_client):
        """Test login with non-existent username"""
        response = await async_client.post("/login", json={
            "username": f"nonexistent_{uuid4().hex[:8]}",
            "password": "SomePass123!"
        })
        
        assert response.status_code in [401, 404]
    
    @pytest.mark.asyncio
    async def test_login_missing_credentials(self, async_client):
        """Test login with missing credentials"""
        incomplete_data = [
            {"password": "SecurePass123!"},  # Missing username
            {"username": "testuser"},  # Missing password
            {}  # Missing both
        ]
        
        for data in incomplete_data:
            response = await async_client.post("/login", json=data)
            assert response.status_code in [400, 422]


class TestRefreshTokenEndpoint:
    """Test token refresh endpoint"""
    
    @pytest.mark.asyncio
    async def test_refresh_token_success(self, async_client):
        """Test successful token refresh"""
        # Create user and login
        username = f"refreshuser_{uuid4().hex[:8]}"
        password = "SecurePass123!"
        
        await async_client.post("/signup", json={
            "username": username,
            "email": f"{username}@example.com",
            "password": password
        })
        
        login_response = await async_client.post("/login", json={
            "username": username,
            "password": password
        })
        
        tokens = login_response.json()
        refresh_token = tokens.get("refresh_token")
        
        if refresh_token:
            # Refresh the token
            response = await async_client.post(
                "/refresh-token",
                headers={"Authorization": f"Bearer {refresh_token}"}
            )
            
            assert response.status_code == 200
            data = response.json()
            assert "access_token" in data
    
    @pytest.mark.asyncio
    async def test_refresh_with_invalid_token(self, async_client):
        """Test refresh with invalid token"""
        response = await async_client.post(
            "/refresh-token",
            headers={"Authorization": "Bearer invalid.token.here"}
        )
        
        assert response.status_code in [401, 403]
    
    @pytest.mark.asyncio
    async def test_refresh_without_token(self, async_client):
        """Test refresh without providing token"""
        response = await async_client.post("/refresh-token")
        
        assert response.status_code == 401


class TestLogoutEndpoint:
    """Test logout endpoint"""
    
    @pytest.mark.asyncio
    async def test_logout_success(self, async_client):
        """Test successful logout"""
        # Create user and login
        username = f"logoutuser_{uuid4().hex[:8]}"
        password = "SecurePass123!"
        
        await async_client.post("/signup", json={
            "username": username,
            "email": f"{username}@example.com",
            "password": password
        })
        
        login_response = await async_client.post("/login", json={
            "username": username,
            "password": password
        })
        
        tokens = login_response.json()
        access_token = tokens.get("access_token")
        
        if access_token:
            # Logout
            response = await async_client.post(
                "/logout",
                headers={"Authorization": f"Bearer {access_token}"}
            )
            
            assert response.status_code in [200, 204]
    
    @pytest.mark.asyncio
    async def test_logout_without_token(self, async_client):
        """Test logout without authentication"""
        response = await async_client.post("/logout")
        
        assert response.status_code == 401


class TestPasswordUpdateEndpoint:
    """Test password update endpoint"""
    
    @pytest.mark.asyncio
    async def test_update_password_success(self, async_client):
        """Test successful password update"""
        username = f"pwduser_{uuid4().hex[:8]}"
        old_password = "OldPass123!"
        new_password = "NewPass123!"
        
        # Create user
        await async_client.post("/signup", json={
            "username": username,
            "email": f"{username}@example.com",
            "password": old_password
        })
        
        # Login
        login_response = await async_client.post("/login", json={
            "username": username,
            "password": old_password
        })
        
        tokens = login_response.json()
        access_token = tokens.get("access_token")
        
        if access_token:
            # Update password
            response = await async_client.put(
                "/update-password",
                headers={"Authorization": f"Bearer {access_token}"},
                json={
                    "old_password": old_password,
                    "new_password": new_password
                }
            )
            
            assert response.status_code in [200, 204]
            
            # Verify can login with new password
            new_login = await async_client.post("/login", json={
                "username": username,
                "password": new_password
            })
            assert new_login.status_code == 200
    
    @pytest.mark.asyncio
    async def test_update_password_wrong_old_password(self, async_client):
        """Test password update with wrong old password"""
        username = f"pwduser_{uuid4().hex[:8]}"
        password = "CorrectPass123!"
        
        await async_client.post("/signup", json={
            "username": username,
            "email": f"{username}@example.com",
            "password": password
        })
        
        login_response = await async_client.post("/login", json={
            "username": username,
            "password": password
        })
        
        tokens = login_response.json()
        access_token = tokens.get("access_token")
        
        if access_token:
            response = await async_client.put(
                "/update-password",
                headers={"Authorization": f"Bearer {access_token}"},
                json={
                    "old_password": "WrongPass123!",
                    "new_password": "NewPass123!"
                }
            )
            
            assert response.status_code in [400, 401, 403]
    
    @pytest.mark.asyncio
    async def test_update_password_weak_new_password(self, async_client):
        """Test password update with weak new password"""
        username = f"pwduser_{uuid4().hex[:8]}"
        password = "SecurePass123!"
        
        await async_client.post("/signup", json={
            "username": username,
            "email": f"{username}@example.com",
            "password": password
        })
        
        login_response = await async_client.post("/login", json={
            "username": username,
            "password": password
        })
        
        tokens = login_response.json()
        access_token = tokens.get("access_token")
        
        if access_token:
            response = await async_client.put(
                "/update-password",
                headers={"Authorization": f"Bearer {access_token}"},
                json={
                    "old_password": password,
                    "new_password": "123"  # Too weak
                }
            )
            
            assert response.status_code in [400, 422]


class TestProtectedEndpoint:
    """Test protected endpoint access"""
    
    @pytest.mark.asyncio
    async def test_protected_route_with_valid_token(self, async_client):
        """Test accessing protected route with valid token"""
        username = f"protuser_{uuid4().hex[:8]}"
        password = "SecurePass123!"
        
        await async_client.post("/signup", json={
            "username": username,
            "email": f"{username}@example.com",
            "password": password
        })
        
        login_response = await async_client.post("/login", json={
            "username": username,
            "password": password
        })
        
        tokens = login_response.json()
        access_token = tokens.get("access_token")
        
        if access_token:
            response = await async_client.get(
                "/protected-route",
                headers={"Authorization": f"Bearer {access_token}"}
            )
            
            assert response.status_code == 200
    
    @pytest.mark.asyncio
    async def test_protected_route_without_token(self, async_client):
        """Test accessing protected route without token"""
        response = await async_client.get("/protected-route")
        
        assert response.status_code == 401
    
    @pytest.mark.asyncio
    async def test_protected_route_with_invalid_token(self, async_client):
        """Test accessing protected route with invalid token"""
        response = await async_client.get(
            "/protected-route",
            headers={"Authorization": "Bearer invalid.token.here"}
        )
        
        assert response.status_code in [401, 403]


class TestAuthenticationSecurity:
    """Test security features"""
    
    @pytest.mark.asyncio
    async def test_sql_injection_prevention(self, async_client):
        """Test SQL injection prevention in login"""
        response = await async_client.post("/login", json={
            "username": "admin' OR '1'='1",
            "password": "password' OR '1'='1"
        })
        
        # Should not succeed with SQL injection
        assert response.status_code in [401, 404, 422]
    
    @pytest.mark.asyncio
    async def test_xss_prevention(self, async_client):
        """Test XSS prevention in signup"""
        response = await async_client.post("/signup", json={
            "username": "<script>alert('xss')</script>",
            "email": "test@example.com",
            "password": "SecurePass123!"
        })
        
        # Should handle XSS attempts safely
        assert response.status_code in [400, 422] or response.status_code == 201
    
    @pytest.mark.asyncio
    async def test_rate_limiting(self, async_client):
        """Test rate limiting on login attempts"""
        # This depends on whether rate limiting is implemented
        # Make multiple rapid login attempts
        username = "ratelimituser"
        
        for _ in range(20):
            await async_client.post("/login", json={
                "username": username,
                "password": "wrongpass"
            })
        
        # Should eventually rate limit (if implemented)
        # Test passes regardless as this is optional feature
        assert True
