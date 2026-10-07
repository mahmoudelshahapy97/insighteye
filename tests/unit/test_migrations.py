import pytest
from unittest.mock import AsyncMock, MagicMock

from app.services.migrations import MIGRATIONS, run_pending_migrations


def _db_with_conn(conn):
    db = MagicMock()
    db.execute_query = AsyncMock()
    tx = MagicMock()
    tx.__aenter__ = AsyncMock(return_value=conn)
    tx.__aexit__ = AsyncMock(return_value=None)
    db.transaction.return_value = tx
    return db


@pytest.mark.asyncio
async def test_pending_migration_runs_and_is_recorded():
    conn = MagicMock()
    conn.fetchval = AsyncMock(return_value=None)
    conn.execute = AsyncMock(return_value="UPDATE 3")
    db = _db_with_conn(conn)

    result = await run_pending_migrations(db)

    assert result["applied"] == [name for name, _ in MIGRATIONS]
    executed = [c.args[0] for c in conn.execute.call_args_list]
    assert any("pg_advisory_xact_lock" in q for q in executed)
    assert any("is_people_counting_camera = TRUE" in q for q in executed)
    assert any("INSERT INTO schema_migrations" in q for q in executed)


@pytest.mark.asyncio
async def test_applied_migration_is_skipped():
    conn = MagicMock()
    conn.fetchval = AsyncMock(return_value=1)
    conn.execute = AsyncMock()
    db = _db_with_conn(conn)

    result = await run_pending_migrations(db)

    assert result["applied"] == []
    executed = [c.args[0] for c in conn.execute.call_args_list]
    assert not any("UPDATE video_stream" in q for q in executed)
