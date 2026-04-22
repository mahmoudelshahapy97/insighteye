# tests/unit/test_redis_batch_service.py
"""
Unit tests for RedisBatchService.

All Redis and PostgreSQL calls are mocked — no live services required.
"""

import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from app.services.redis_batch_service import RedisBatchService


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_detection(**overrides):
    defaults = dict(
        stream_id=str(uuid4()),
        workspace_id=str(uuid4()),
        user_id=str(uuid4()),
        camera_name="Cam-01",
        username="testuser",
        person_count=3,
        male_count=2,
        female_count=1,
        fire_status="no detection",
        location_info={"location": "Main Hall", "building": "HQ"},
    )
    defaults.update(overrides)
    return defaults


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def svc():
    """Return a fresh RedisBatchService with an injected mock Redis client."""
    service = RedisBatchService()
    service._connected = True
    service._client = AsyncMock()
    return service


# ---------------------------------------------------------------------------
# push_detection
# ---------------------------------------------------------------------------

class TestPushDetection:

    @pytest.mark.asyncio
    async def test_push_serialises_required_fields(self, svc):
        """push_detection should RPUSH a JSON record with all key fields."""
        d = _make_detection()
        svc._client.rpush = AsyncMock(return_value=1)

        result = await svc.push_detection(**d)

        assert result is True
        svc._client.rpush.assert_awaited_once()
        key, raw = svc._client.rpush.call_args[0]

        assert "detection_batch" in key
        assert d["workspace_id"] in key
        assert d["stream_id"] in key

        parsed = json.loads(raw)
        assert parsed["camera_id"] == d["stream_id"]
        assert parsed["person_count"] == d["person_count"]
        assert parsed["fire_status"] == d["fire_status"]
        assert parsed["location_info"] == d["location_info"]

    @pytest.mark.asyncio
    async def test_push_returns_false_when_not_connected(self, svc):
        """Should return False immediately if Redis is not connected."""
        svc._connected = False
        result = await svc.push_detection(**_make_detection())
        assert result is False
        svc._client.rpush.assert_not_called()

    @pytest.mark.asyncio
    async def test_push_returns_false_on_redis_error(self, svc):
        """Should return False (not raise) when Redis raises an exception."""
        svc._client.rpush = AsyncMock(side_effect=ConnectionError("Redis down"))
        result = await svc.push_detection(**_make_detection())
        assert result is False
        assert svc._connected is False  # Mark as disconnected


# ---------------------------------------------------------------------------
# flush_all_to_postgres
# ---------------------------------------------------------------------------

class TestFlushAllToPostgres:

    def _build_pipeline_mock(self, raw_items, delete_count=1):
        """Return a mock async context manager for pipeline()."""
        pipe = AsyncMock()
        pipe.lrange = MagicMock()
        pipe.delete = MagicMock()
        pipe.execute = AsyncMock(return_value=[raw_items, delete_count])
        # Make the pipeline() call return an async ctx manager
        cm = AsyncMock()
        cm.__aenter__ = AsyncMock(return_value=pipe)
        cm.__aexit__ = AsyncMock(return_value=False)
        return cm

    @pytest.mark.asyncio
    async def test_flush_calls_batch_insert_with_records(self, svc):
        """flush_all_to_postgres should call batch_insert_detection_data once."""
        ws_id = str(uuid4())
        stream_id = str(uuid4())
        key = f"detection_batch:{ws_id}:{stream_id}"

        d = _make_detection(workspace_id=ws_id, stream_id=stream_id)
        raw_items = [json.dumps({**d, "camera_id": d["stream_id"]})]

        svc._client.keys = AsyncMock(return_value=[key])
        svc._client.pipeline = MagicMock(
            return_value=self._build_pipeline_mock(raw_items)
        )

        mock_result = {"success": True, "inserted_count": 1}
        with patch(
            "app.services.redis_batch_service.postgres_service"
            if False else "app.services.postgres_service.postgres_service",
            create=True,
        ):
            with patch(
                "app.services.redis_batch_service.RedisBatchService.flush_all_to_postgres",
                wraps=svc.flush_all_to_postgres,
            ):
                pass

        # Direct patch of the import inside the service
        mock_pg = AsyncMock()
        mock_pg.batch_insert_detection_data = AsyncMock(return_value=mock_result)

        with patch(
            "app.services.redis_batch_service.postgres_service",
            mock_pg,
        ):
            total = await svc.flush_all_to_postgres()

        mock_pg.batch_insert_detection_data.assert_awaited_once()
        assert total == 1

    @pytest.mark.asyncio
    async def test_flush_deletes_key_after_reading(self, svc):
        """flush_all_to_postgres should DELETE the Redis key after reading."""
        ws_id = str(uuid4())
        stream_id = str(uuid4())
        key = f"detection_batch:{ws_id}:{stream_id}"

        d = _make_detection(workspace_id=ws_id, stream_id=stream_id)
        raw_items = [json.dumps({**d, "camera_id": d["stream_id"]})]

        pipeline_cm = self._build_pipeline_mock(raw_items)
        inner_pipe = None

        async def capture_pipe():
            nonlocal inner_pipe
            inner_pipe = pipeline_cm.__aenter__.return_value
            return inner_pipe

        svc._client.keys = AsyncMock(return_value=[key])
        svc._client.pipeline = MagicMock(return_value=pipeline_cm)

        mock_pg = AsyncMock()
        mock_pg.batch_insert_detection_data = AsyncMock(
            return_value={"success": True, "inserted_count": 1}
        )

        with patch("app.services.redis_batch_service.postgres_service", mock_pg):
            await svc.flush_all_to_postgres()

        # Delete must be called in the pipeline (as part of the atomic transaction)
        pipeline_cm.__aenter__.return_value.delete.assert_called_once_with(key)

    @pytest.mark.asyncio
    async def test_flush_returns_zero_when_no_keys(self, svc):
        """Should return 0 and not call batch_insert when Redis is empty."""
        svc._client.keys = AsyncMock(return_value=[])
        mock_pg = AsyncMock()

        with patch("app.services.redis_batch_service.postgres_service", mock_pg):
            total = await svc.flush_all_to_postgres()

        assert total == 0
        mock_pg.batch_insert_detection_data.assert_not_called()

    @pytest.mark.asyncio
    async def test_flush_returns_zero_when_not_connected(self, svc):
        """Should bail out immediately if Redis is not connected."""
        svc._connected = False
        svc._client = None
        mock_pg = AsyncMock()

        with patch("app.services.redis_batch_service.postgres_service", mock_pg):
            total = await svc.flush_all_to_postgres()

        assert total == 0


# ---------------------------------------------------------------------------
# Redis failure fallback (integration level — tests detection_data_service)
# ---------------------------------------------------------------------------

class TestRedisFailureFallback:

    @pytest.mark.asyncio
    async def test_redis_failure_triggers_direct_pg_insert(self):
        """
        When redis_batch_service.push_detection() returns False, 
        detection_data_service.insert_detection_data() must call
        postgres_service.insert_detection_data() directly.
        """
        from app.services.detection_data_service import DetectionDataService



        mock_pg = AsyncMock()
        mock_pg.insert_detection_data = AsyncMock(return_value=True)

        mock_redis_svc = AsyncMock()
        mock_redis_svc.push_detection = AsyncMock(return_value=False)  # Redis down

        import numpy as np

        svc = DetectionDataService()

        svc.postgres_service = mock_pg

        with patch(
            "app.services.detection_data_service._get_redis_batch_service",
            return_value=mock_redis_svc,
        ):
            result = await svc.insert_detection_data(
                stream_id=str(uuid4()),
                workspace_id=str(uuid4()),
                user_id=str(uuid4()),
                camera_name="Test Cam",
                username="user",
                person_count=2,
                male_count=1,
                female_count=1,
                fire_status="no detection",
                frame=np.zeros((100, 100, 3), dtype=np.uint8),
            )

        assert result is True
        mock_pg.insert_detection_data.assert_awaited_once()
