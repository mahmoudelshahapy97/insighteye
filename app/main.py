# /app/main.py
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging
import signal
import uvicorn
from app.config.settings import config
from app.utils.logging_config import setup_logging

from app.api.routes import router
from app.services.stream_service import stream_manager, initialize_stream_manager
from app.services.stream_processing_service import stream_processing_service
from app.services.distributed_stream_manager import (
    distributed_stream_manager,
    initialize_distributed_stream_manager,
)
from app.services.redis_batch_service import redis_batch_service

from zoneinfo import ZoneInfo
from datetime import datetime
from app.services.database import (
    init_db_pool, close_db_pool, connection_pool, 
    get_pool,
    check_postgres_health
)

setup_logging(config.log_file_path)
logger = logging.getLogger(__name__)
logger.info("Logging initialized successfully!")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Application startup sequence initiated (async)...")

    try:
        await init_db_pool()
        logger.info("Asyncpg database connection pool initialization requested.")
    except Exception as e:
        logger.critical(f"CRITICAL: Failed to initialize asyncpg pool: {e}", exc_info=True)
        raise RuntimeError("Database pool initialization failed.") from e

    try:
        healthy = await check_postgres_health()
    except Exception as e:
        logger.critical("CRITICAL: DB health check errored after pool init: %s", e, exc_info=True)
        raise RuntimeError("Database health check failed after pool init.") from e

    if not healthy:
        logger.critical("CRITICAL: DB health check reported unhealthy after pool init.")
        raise RuntimeError("Database health check failed after pool init.")

    try:
        from app.services.database import db_manager as _db
        await _db.execute_query(
            "ALTER TABLE shoplifting_events ADD COLUMN IF NOT EXISTS video_path VARCHAR(500)",
            fetch_one=False,
        )
        logger.info("shoplifting_events.video_path column ensured.")
    except Exception as e:
        logger.warning("Could not add video_path column: %s", e)

    try:
        from app.services.database import db_manager as _db
        await _db.execute_query(
            """
            CREATE TABLE IF NOT EXISTS threshold_violations (
                violation_id UUID PRIMARY KEY,
                stream_id UUID NOT NULL,
                workspace_id UUID NOT NULL,
                person_count INT NOT NULL,
                threshold_type VARCHAR(20) NOT NULL,
                threshold_value INT NOT NULL,
                "timestamp" TIMESTAMPTZ NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """,
            fetch_one=False,
        )
        await _db.execute_query(
            """
            CREATE INDEX IF NOT EXISTS idx_threshold_violations_workspace_stream
                ON threshold_violations (workspace_id, stream_id, "timestamp")
            """,
            fetch_one=False,
        )
        logger.info("threshold_violations table ensured.")
    except Exception as e:
        logger.warning("Could not create threshold_violations table: %s", e)

    try:
        from app.services.database import db_manager as _db
        await _db.execute_query(
            "CREATE INDEX IF NOT EXISTS idx_stream_results_workspace_timestamp "
            "ON stream_results (workspace_id, timestamp)",
            fetch_one=False,
        )
        logger.info("stream_results workspace/timestamp index ensured.")
    except Exception as e:
        logger.warning("Could not create stream_results index: %s", e)

    try:
        stream_processing_service._initialize_models()
        logger.info("Stream manager initialized and background tasks started.")
    except Exception as e:
        logger.error(f"Failed to initialize StreamManager: {e}", exc_info=True)

    try:
        await initialize_stream_manager() 
        logger.info("Stream manager initialized and background tasks started.")
    except Exception as e:
        logger.error(f"Failed to initialize StreamManager: {e}", exc_info=True)

    try:
        await initialize_distributed_stream_manager() 
        logger.info("Stream manager initialized and background tasks started.")
    except Exception as e:
        logger.error(f"Failed to initialize StreamManager: {e}", exc_info=True)

    # Start Redis batch flush loop (buffers detections → PG every 5 min)
    try:
        await redis_batch_service.start_flush_loop()
        logger.info("Redis detection batch flush loop started.")
    except Exception as e:
        logger.error(f"Failed to start Redis batch flush loop: {e}", exc_info=True)

    logger.info("Application startup complete (async).")
    yield
    # Shutdown
    logger.info("Application shutdown sequence initiated (async)...")

    # Stop the distributed manager first. Kept in its own try block so a failure
    # here cannot skip the stream_manager shutdown below.
    try:
        await distributed_stream_manager.stop_management_loop()
        logger.info("Distributed stream manager stopped.")
    except Exception as e:
        logger.error(f"Error stopping distributed stream manager: {e}", exc_info=True)

    if stream_manager:
        try:
            await stream_manager.shutdown() # Already async
            logger.info("Stream manager shutdown complete.")
        except Exception as e:
            logger.error(f"Error during StreamManager shutdown: {e}", exc_info=True)

    # Final Redis flush → PostgreSQL before closing DB pool
    try:
        await redis_batch_service.stop_flush_loop()
        logger.info("Redis batch flush loop stopped and final flush complete.")
    except Exception as e:
        logger.error(f"Error stopping Redis batch service: {e}", exc_info=True)

    if connection_pool and not connection_pool._closed: # Check if pool exists and not closed
        try:
            await close_db_pool()
            logger.info("Asyncpg database connection pool closed.")
        except Exception as e:
            logger.error(f"Error closing asyncpg database connection pool: {e}", exc_info=True)

    logger.info("Application shutdown complete (async).")

from starlette.middleware.base import BaseHTTPMiddleware
class CacheControlMiddleware(BaseHTTPMiddleware): 
    async def dispatch(self, request, call_next):
        response = await call_next(request)
        static_path = config.static_path_prefix
        api_prefix = config.api_path_prefix
        auth_prefix = config.auth_path_prefix

        if request.url.path.startswith(static_path):
            response.headers['Cache-Control'] = config.static_cache_control
        elif request.url.path.startswith(api_prefix) or request.url.path.startswith(auth_prefix):
            response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, proxy-revalidate'
        return response

app = FastAPI(
    root_path=config.fastapi_root_path,
    lifespan=lifespan,
    title=config.app_name,
    description=config.app_description,
    version=config.app_version,
    # NOTE: these must be explicitly None to disable. Commenting them out does not
    # disable the docs, it restores FastAPI's defaults (/docs, /redoc, /openapi.json).
    # root_path is applied automatically, so these are declared unprefixed.
    openapi_url=None if config.environment == "production" else "/openapi.json",
    docs_url=None if config.environment == "production" else "/docs",
    redoc_url=None if config.environment == "production" else "/redoc",
)

app.add_middleware(
    CORSMiddleware,
    # A wildcard here combined with allow_credentials=True lets any origin make
    # authenticated cross-origin calls and read the responses. Use the allowlist.
    allow_origins=config.cors_origins,
    allow_credentials=config.cors_allow_credentials,
    allow_methods=config.cors_allow_methods,
    allow_headers=config.cors_allow_headers,
    expose_headers=config.cors_expose_headers
)

@app.middleware("http")
async def add_security_headers(request: Request, call_next): # Identical to main.py
    response = await call_next(request)
    is_secure_scheme = request.url.scheme == "https"
    x_forwarded_proto = request.headers.get("x-forwarded-proto")
    is_behind_secure_proxy = x_forwarded_proto == "https"

    if is_secure_scheme or is_behind_secure_proxy or config.force_hsts:
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains; preload"

    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"

    if "server" in response.headers:
        del response.headers["server"]
    return response

@app.get("/health", tags=["System"])#, include_in_schema=False
async def health_check():
    """API health check endpoint with detailed diagnostics"""
    db_healthy = False
    conn_info = "DB pool not initialized"
    pool_stats = {}

    try:
        connection_pool = get_pool()

        if connection_pool:
            if connection_pool._closed:
                conn_info = "DB pool is closed"
            else:
                try:
                    async with connection_pool.acquire() as conn:
                        await conn.fetchval("SELECT 1")
                    db_healthy = True
                    pool_stats = {
                        "size": connection_pool.get_size(),
                        "idle": connection_pool.get_idle_size(),
                        "max": connection_pool.get_max_size(),
                    }
                    conn_info = f"DB pool healthy (size: {pool_stats['size']}, idle: {pool_stats['idle']}/{pool_stats['max']})"
                except Exception as e:
                    conn_info = f"DB pool error: {str(e)}"
                    logger.warning(f"Health check: DB connection failed - {e}", exc_info=True)
        else:
            conn_info = "DB pool is None"
    except Exception as e:
        conn_info = f"Unexpected error: {str(e)}"
        logger.error(f"Health check error: {e}", exc_info=True)

    return {
        "status": "healthy" if db_healthy else "degraded",
        "timestamp": datetime.now(ZoneInfo("Africa/Cairo")).isoformat(),
        "database_status": conn_info,
        "db_connection_ok": db_healthy,
        "pool_stats": pool_stats if pool_stats else None,
    }

app.include_router(router)

def signal_handler(signum, frame):
    logger.info(f"Signal {signal.Signals(signum).name} received. Initiating graceful shutdown via Uvicorn.")

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


if __name__ == "__main__":

    uvicorn.run(
        "main:app", 
        host=config.app_host,
        port=config.app_port,
    )
    