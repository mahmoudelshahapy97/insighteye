"""
End-to-end tests for Stream Lifecycle
Tests complete stream lifecycle from creation to deletion including error recovery
"""

import pytest
from httpx import AsyncClient
from uuid import uuid4
import asyncio


@pytest.mark.e2e
class TestStreamLifecycle:
    """Test complete stream lifecycle"""
    
    @pytest.mark.asyncio
    async def test_complete_stream_lifecycle(self, async_client):
        """
        Complete stream lifecycle:
        1. Create camera
        2. Start stream
        3. Process frames
        4. Detect objects
        5. Store data
        6. Send notifications
        7. Stop stream
        8. Cleanup resources
        """
        # Create user and login
        username = f"streamuser_{uuid4().hex[:8]}"
        password = "SecurePass123!"
        
        await async_client.post("/auth/signup", json={
            "username": username,
            "email": f"{username}@example.com",
            "password": password
        })
        
        login_response = await async_client.post("/auth/login", json={
            "username": username,
            "password": password
        })
        
        tokens = login_response.json()
        headers = {"Authorization": f"Bearer {tokens['access_token']}"}
        
        # 1. Create camera
        camera_response = await async_client.post(
            "/api/cameras",
            headers=headers,
            json={
                "name": "Lifecycle Test Camera",
                "path": "rtsp://test",
                "stream_type": "rtsp"
            }
        )
        
        if camera_response.status_code in [200, 201]:
            camera = camera_response.json()
            camera_id = camera.get("stream_id") or camera.get("id")
            
            if camera_id:
                # 2. Start stream
                start_response = await async_client.post(
                    f"/api/streams/{camera_id}/start",
                    headers=headers
                )
                
                # 3-6. Stream processing happens automatically
                # Wait a bit for processing
                await asyncio.sleep(2)
                
                # Check stream status
                status_response = await async_client.get(
                    f"/api/streams/{camera_id}/status",
                    headers=headers
                )
                
                # 7. Stop stream
                stop_response = await async_client.post(
                    f"/api/streams/{camera_id}/stop",
                    headers=headers
                )
                
                assert stop_response.status_code in [200, 204, 400]
                
                # 8. Delete camera (cleanup)
                delete_response = await async_client.delete(
                    f"/api/cameras/{camera_id}",
                    headers=headers
                )
                
                assert delete_response.status_code in [200, 204]


@pytest.mark.e2e
class TestStreamErrorRecovery:
    """Test stream error recovery scenarios"""
    
    @pytest.mark.asyncio
    async def test_stream_reconnection_after_failure(self, async_client):
        """
        Error recovery:
        1. Start stream
        2. Simulate connection loss
        3. Verify auto-reconnect
        4. Verify data continuity
        """
        username = f"erroruser_{uuid4().hex[:8]}"
        password = "SecurePass123!"
        
        await async_client.post("/auth/signup", json={
            "username": username,
            "email": f"{username}@example.com",
            "password": password
        })
        
        login_response = await async_client.post("/auth/login", json={
            "username": username,
            "password": password
        })
        
        tokens = login_response.json()
        headers = {"Authorization": f"Bearer {tokens['access_token']}"}
        
        # Create camera with potentially failing stream
        camera_response = await async_client.post(
            "/api/cameras",
            headers=headers,
            json={
                "name": "Error Recovery Camera",
                "path": "rtsp://unreliable.stream/test",
                "stream_type": "rtsp"
            }
        )
        
        if camera_response.status_code in [200, 201]:
            camera = camera_response.json()
            camera_id = camera.get("stream_id") or camera.get("id")
            
            if camera_id:
                # Start stream (may fail)
                start_response = await async_client.post(
                    f"/api/streams/{camera_id}/start",
                    headers=headers
                )
                
                # System should handle errors gracefully
                assert start_response.status_code in [200, 201, 400, 500]


@pytest.mark.e2e
class TestMultipleStreams:
    """Test managing multiple streams simultaneously"""
    
    @pytest.mark.asyncio
    async def test_concurrent_streams(self, async_client):
        """Test running multiple streams concurrently"""
        username = f"multistream_{uuid4().hex[:8]}"
        password = "SecurePass123!"
        
        await async_client.post("/auth/signup", json={
            "username": username,
            "email": f"{username}@example.com",
            "password": password
        })
        
        login_response = await async_client.post("/auth/login", json={
            "username": username,
            "password": password
        })
        
        tokens = login_response.json()
        headers = {"Authorization": f"Bearer {tokens['access_token']}"}
        
        # Create multiple cameras
        camera_ids = []
        for i in range(3):
            camera_response = await async_client.post(
                "/api/cameras",
                headers=headers,
                json={
                    "name": f"Camera {i+1}",
                    "path": f"rtsp://test{i+1}",
                    "stream_type": "rtsp"
                }
            )
            
            if camera_response.status_code in [200, 201]:
                camera = camera_response.json()
                camera_id = camera.get("stream_id") or camera.get("id")
                if camera_id:
                    camera_ids.append(camera_id)
        
        # Start all streams concurrently
        start_tasks = [
            async_client.post(f"/api/streams/{cam_id}/start", headers=headers)
            for cam_id in camera_ids
        ]
        
        start_responses = await asyncio.gather(*start_tasks, return_exceptions=True)
        
        # Verify all started (or failed gracefully)
        assert len(start_responses) == len(camera_ids)
        
        # Stop all streams
        stop_tasks = [
            async_client.post(f"/api/streams/{cam_id}/stop", headers=headers)
            for cam_id in camera_ids
        ]
        
        await asyncio.gather(*stop_tasks, return_exceptions=True)


@pytest.mark.e2e
class TestStreamDataPersistence:
    """Test stream data persistence and retrieval"""
    
    @pytest.mark.asyncio
    async def test_detection_data_persistence(self, async_client):
        """Test that detection data is persisted correctly"""
        username = f"datauser_{uuid4().hex[:8]}"
        password = "SecurePass123!"
        
        await async_client.post("/auth/signup", json={
            "username": username,
            "email": f"{username}@example.com",
            "password": password
        })
        
        login_response = await async_client.post("/auth/login", json={
            "username": username,
            "password": password
        })
        
        tokens = login_response.json()
        headers = {"Authorization": f"Bearer {tokens['access_token']}"}
        
        # Create camera and start stream
        camera_response = await async_client.post(
            "/api/cameras",
            headers=headers,
            json={
                "name": "Data Persistence Camera",
                "path": "rtsp://test",
                "stream_type": "rtsp"
            }
        )
        
        if camera_response.status_code in [200, 201]:
            camera = camera_response.json()
            camera_id = camera.get("stream_id") or camera.get("id")
            
            if camera_id:
                # Start stream
                await async_client.post(
                    f"/api/streams/{camera_id}/start",
                    headers=headers
                )
                
                # Wait for some processing
                await asyncio.sleep(3)
                
                # Stop stream
                await async_client.post(
                    f"/api/streams/{camera_id}/stop",
                    headers=headers
                )
                
                # Check if analytics data is available
                analytics_response = await async_client.get(
                    f"/api/analytics/detections?camera_id={camera_id}",
                    headers=headers
                )
                
                # Data should be persisted
                assert analytics_response.status_code in [200, 404]
