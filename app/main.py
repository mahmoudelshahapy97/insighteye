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
from app.services.distributed_stream_manager import initialize_distributed_stream_manager

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
        if not healthy:
            raise RuntimeError("DB health check failed after pool init")
    except Exception as e:
        logger.warning("Index/health check failed: %s", e)
    
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

    logger.info("Application startup complete (async).")
    yield
    # Shutdown
    logger.info("Application shutdown sequence initiated (async)...")

    if stream_manager:
        try:

            # Stop distributed manager first
            await distributed_stream_manager.stop_management_loop()

            await stream_manager.shutdown() # Already async
            logger.info("Stream manager shutdown complete.")
        except Exception as e:
            logger.error(f"Error during StreamManager shutdown: {e}", exc_info=True)

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
    # openapi_url=f"{config.fastapi_root_path}/openapi.json",
    # docs_url=f"{config.fastapi_root_path}/docs",
    # redoc_url=f"{config.fastapi_root_path}/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
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
    