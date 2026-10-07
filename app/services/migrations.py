# app/services/migrations.py
"""One-time data migrations, recorded in schema_migrations so each runs once.

schema_migrations is shared with the external SQL-file runner (filename,
checksum, applied_at); app migrations are recorded with an "app:" prefix.

Use this for backfills that must not repeat on every startup (unlike the
idempotent ADD COLUMN IF NOT EXISTS blocks in app/main.py), e.g. turning a flag
on for legacy rows that a user may later turn off again.
"""
import hashlib
import logging
from typing import Dict, List, Tuple

logger = logging.getLogger(__name__)

# Arbitrary constant: serialises migration runs across app workers starting together.
MIGRATIONS_LOCK_KEY = 7_316_220_041

LEDGER_DDL = """
    CREATE TABLE IF NOT EXISTS schema_migrations (
        filename TEXT PRIMARY KEY,
        checksum TEXT,
        applied_at TIMESTAMPTZ DEFAULT NOW()
    )
"""

# (name, [statements]) in apply order. Never edit an applied migration; add a new one.
MIGRATIONS: List[Tuple[str, List[str]]] = [
    (
        # Cameras created before the per-camera feature gates existed: their
        # people-count alerts were configured but is_people_counting_camera
        # defaulted to FALSE, so alerts were silently skipped. Also realign the
        # legacy is_shoplifting_camera flag with detection_models, which is what
        # actually gates shoplifting detection.
        "app:2026_10_legacy_camera_flags",
        [
            """
            UPDATE video_stream SET is_people_counting_camera = TRUE
            WHERE NOT is_people_counting_camera AND alert_enabled
              AND (count_threshold_greater IS NOT NULL OR count_threshold_less IS NOT NULL)
            """,
            """
            UPDATE video_stream
            SET is_shoplifting_camera = ('shoplifting' = ANY(COALESCE(detection_models, '{}')))
            WHERE is_shoplifting_camera IS DISTINCT FROM ('shoplifting' = ANY(COALESCE(detection_models, '{}')))
            """,
        ],
    ),
]


async def run_pending_migrations(db_manager) -> Dict[str, List[str]]:
    """Apply every migration not yet in schema_migrations, each in its own transaction."""
    await db_manager.execute_query(LEDGER_DDL, fetch_one=False)

    applied, skipped = [], []
    for name, statements in MIGRATIONS:
        async with db_manager.transaction() as conn:
            await conn.execute("SELECT pg_advisory_xact_lock($1)", MIGRATIONS_LOCK_KEY)
            if await conn.fetchval("SELECT 1 FROM schema_migrations WHERE filename = $1", name):
                skipped.append(name)
                continue
            for stmt in statements:
                status = await conn.execute(stmt)
                logger.info("Migration %s: %s", name, status)
            checksum = hashlib.sha256("\n".join(statements).encode()).hexdigest()
            await conn.execute(
                "INSERT INTO schema_migrations (filename, checksum) VALUES ($1, $2)", name, checksum
            )
            applied.append(name)
            logger.info("Migration %s applied.", name)

    if skipped:
        logger.info("Migrations already applied: %s", ", ".join(skipped))
    return {"applied": applied, "skipped": skipped}
