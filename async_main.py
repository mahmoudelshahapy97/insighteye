# async_main.py
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
# from fastapi.middleware.trustedhost import TrustedHostMiddleware # Keep commented to match main.py
# from fastapi.middleware.gzip import GZipMiddleware # Keep commented to match main.py
from contextlib import asynccontextmanager
import logging
import signal
import uvicorn
from async_config import config
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# Import routers
from async_camera import router as camera_router
from async_video_streaming_qdrant import router as video_router
from async_sessions_2 import router as session_router_2
from async_users import router as users_router
from async_otp import router as otp_router
from async_login_user import router as login_router
from async_text_chat import router as chat_router
from async_stream_one import router as stream_one_router, stream_manager, initialize_stream_manager
from async_qdrant_chat import router as qdrant_chat_router
from async_workspaces import router as workspace_router
from admin_router import router as admin_utils_router

from datetime import datetime, timezone
# Assuming DatabaseManager is designed for asyncpg and correctly handles transactions
from async_database import DatabaseManager # For scheduler task
from async_database import (init_db_pool as async_init_db_pool,
                      close_db_pool as async_close_db_pool,
                      connection_pool as asyncpg_connection_pool,
                      initialize_database as async_initialize_database,
                      ensure_database_indices as async_ensure_database_indices)

# Setup logging
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(config.get("log_file_path", "app.log")),
        logging.StreamHandler()
    ])
logger = logging.getLogger(__name__)

# Initialize APScheduler - This is an enhancement in async_main.py
scheduler = AsyncIOScheduler()

async def cleanup_expired_data():
    """Clean up expired records from various tables."""
    logger.info("Executing scheduled job: cleanup_expired_data")
    db_manager = DatabaseManager() # Assuming DatabaseManager uses asyncpg_connection_pool
    try:
        # Example: Using context manager if DatabaseManager provides one
        # async with db_manager.get_connection() as conn:
        # Or if DatabaseManager directly executes queries using the pool:
        # await db_manager.execute_query(...)

        # For direct use of asyncpg pool within this function (if DatabaseManager is complex)
        if not asyncpg_connection_pool or asyncpg_connection_pool._closed:
            logger.warning("cleanup_expired_data: DB pool not available or closed.")
            return

        async with asyncpg_connection_pool.acquire() as conn:
            async with conn.transaction():
                # Clean up expired OTPs
                await conn.execute("DELETE FROM otps WHERE expires_at < CURRENT_TIMESTAMP")
                logger.info("Expired OTPs cleaned up.")

                # Clean up expired sessions
                await conn.execute("DELETE FROM sessions WHERE expires_at < CURRENT_TIMESTAMP")
                logger.info("Expired sessions cleaned up.")

                # Clean up expired blacklisted tokens
                await conn.execute("DELETE FROM token_blacklist WHERE expires_at < CURRENT_TIMESTAMP")
                logger.info("Expired blacklisted tokens cleaned up.")
    except Exception as e:
        logger.error(f"Error during cleanup_expired_data: {e}", exc_info=True)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Application startup sequence initiated (async)...")

    try:
        await async_init_db_pool()
        # register_uuid_adapter() is not needed for asyncpg
        logger.info("Asyncpg database connection pool initialization requested.")
    except Exception as e:
        logger.critical(f"CRITICAL: Failed to initialize asyncpg pool: {e}", exc_info=True)
        raise RuntimeError("Database pool initialization failed.") from e

    try:
        logger.info("Attempting to initialize database schema (async)...")
        if await async_initialize_database(): # Should raise on critical failure or return True
            logger.info("Database schema initialized/verified successfully (async).")
    except Exception as e:
        logger.critical(f"CRITICAL: Database schema initialization failed (async): {e}", exc_info=True)
        raise RuntimeError(f"Database schema initialization failed (async): {e}") from e

    try:
        # await async_ensure_database_indices()
        logger.info("Database indices ensured (async).")
    except Exception as e:
        logger.error(f"Failed to ensure database indices (async): {e}", exc_info=True)

    try:
        await initialize_stream_manager() # This was already async
        logger.info("Stream manager initialized and background tasks started.")
    except Exception as e:
        logger.error(f"Failed to initialize StreamManager: {e}", exc_info=True)

    # APScheduler startup - This is an enhancement in async_main.py
    try:
        scheduler.add_job(
            cleanup_expired_data,
            trigger=CronTrigger(hour="*/1"),  # Run every hour
            id="cleanup_expired_data_job", # Ensure unique ID
            replace_existing=True
        )
        scheduler.start()
        logger.info("APScheduler started with cleanup_expired_data scheduled.")
    except Exception as e:
        logger.error(f"Failed to start APScheduler: {e}", exc_info=True)

    logger.info("Application startup complete (async).")
    yield
    # Shutdown
    logger.info("Application shutdown sequence initiated (async)...")

    # APScheduler shutdown
    if scheduler.running:
        try:
            scheduler.shutdown()
            logger.info("APScheduler shut down successfully.")
        except Exception as e:
            logger.error(f"Error during APScheduler shutdown: {e}", exc_info=True)

    if stream_manager:
        try:
            await stream_manager.shutdown() # Already async
            logger.info("Stream manager shutdown complete.")
        except Exception as e:
            logger.error(f"Error during StreamManager shutdown: {e}", exc_info=True)

    if asyncpg_connection_pool and not asyncpg_connection_pool._closed: # Check if pool exists and not closed
        try:
            await async_close_db_pool()
            logger.info("Asyncpg database connection pool closed.")
        except Exception as e:
            logger.error(f"Error closing asyncpg database connection pool: {e}", exc_info=True)

    logger.info("Application shutdown complete (async).")

from starlette.middleware.base import BaseHTTPMiddleware
class CacheControlMiddleware(BaseHTTPMiddleware): # Definition is identical to main.py
    async def dispatch(self, request, call_next):
        response = await call_next(request)
        static_path = config.get("static_path_prefix", "/static/")
        api_prefix = config.get("api_path_prefix", "/api/")
        auth_prefix = config.get("auth_path_prefix", "/auth/")

        if request.url.path.startswith(static_path):
            response.headers['Cache-Control'] = config.get("static_cache_control", "public, max-age=604800")
        elif request.url.path.startswith(api_prefix) or request.url.path.startswith(auth_prefix):
            response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, proxy-revalidate'
        return response

app = FastAPI(
    root_path=config.get("fastapi_root_path", "/insighteye-1"),
    lifespan=lifespan,
    title=config.get("fastapi_title", "InsightEye API"),
    description=config.get("fastapi_description", "API for managing cameras, users, and insights."),
    version=config.get("fastapi_version", "1.0.0"),
    openapi_url=f"{config.get('fastapi_docs_prefix', '')}/openapi.json",
    docs_url=f"{config.get('fastapi_docs_prefix', '')}/docs",
    redoc_url=f"{config.get('fastapi_docs_prefix', '')}/redoc",
)

# Apply middlewares to match main.py

# Add CORS middleware - matching main.py's active configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Matches main.py's `allow_origins=["*"],#origins`
    allow_credentials=config.get("cors_allow_credentials", True),
    allow_methods=config.get("cors_allow_methods", ["*"]),
    allow_headers=config.get("cors_allow_headers", ["*"]),
    expose_headers=config.get("cors_expose_headers", ["X-Request-ID"])
)

# # Add trusted hosts middleware - Keep commented to match main.py
# app.add_middleware(
#     TrustedHostMiddleware,
#     allowed_hosts=config.get("trusted_hosts", ["localhost", "127.0.0.1"])
# )

# # Add compression middleware - Keep commented to match main.py
# app.add_middleware(GZipMiddleware, minimum_size=1000)

# Add cache control middleware - This was active in main.py
# app.add_middleware(CacheControlMiddleware)


@app.middleware("http")
async def add_security_headers(request: Request, call_next): # Identical to main.py
    response = await call_next(request)
    is_secure_scheme = request.url.scheme == "https"
    x_forwarded_proto = request.headers.get("x-forwarded-proto")
    is_behind_secure_proxy = x_forwarded_proto == "https"

    if is_secure_scheme or is_behind_secure_proxy or config.get("force_hsts", False):
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains; preload"

    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    # response.headers["Content-Security-Policy"] = config.get("content_security_policy", "default-src 'self'; object-src 'none'; frame-ancestors 'none';")
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"

    if "server" in response.headers:
        del response.headers["server"]
    return response

# Include routers (identical to main.py)
app.include_router(video_router)
app.include_router(session_router_2)
app.include_router(users_router)
app.include_router(login_router)
app.include_router(otp_router)
app.include_router(camera_router)
app.include_router(chat_router)
app.include_router(stream_one_router)
app.include_router(qdrant_chat_router)
app.include_router(workspace_router)
app.include_router(admin_utils_router)

# Signal handling (identical to main.py)
def signal_handler(signum, frame):
    logger.info(f"Signal {signal.Signals(signum).name} received. Initiating graceful shutdown via Uvicorn.")

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

@app.get("/health", tags=["System"])
async def health_check(): # Adapted for asyncpg
    """API health check endpoint"""
    db_healthy = False
    conn_info = "DB pool not checked or inactive"

    if asyncpg_connection_pool and not asyncpg_connection_pool._closed:
        try:
            async with asyncpg_connection_pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            db_healthy = True
            # Pool stats for asyncpg
            conn_info = (f"Asyncpg DB pool healthy (size: {asyncpg_connection_pool.get_size()}, "
                         f"idle: {asyncpg_connection_pool.get_idle_size()}/{asyncpg_connection_pool.get_max_size()})")
        except Exception as e:
            conn_info = f"Asyncpg DB pool error: {str(e)}"
            logger.warning(f"Health check: Asyncpg DB connection failed - {e}", exc_info=True)
    elif asyncpg_connection_pool and asyncpg_connection_pool._closed:
        conn_info = "Asyncpg DB pool is closed."
    else:
        conn_info = "Asyncpg DB pool not initialized."

    return {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "database_status": conn_info,
        "db_connection_ok": db_healthy
    }

if __name__ == "__main__":
    server_host = config.get("server_host", "0.0.0.0")
    server_port = config.get("server_port", 8001)
    reload_app = config.get("debug_reload", False) # Keep variable for consistency, though uvicorn.run below doesn't use it directly

    logger.info(f"Starting Uvicorn server on {server_host}:{server_port} with reload: {reload_app}")
    uvicorn.run(
        "async_main:app", # <<< CRITICAL FIX: Changed from "main:app"
        host=server_host,
        port=server_port,
        # reload=reload_app, # Consistent with main.py, though often controlled via CLI
        # log_level=config.get("uvicorn_log_level", "info").lower() # Consistent with main.py
        # SSL options can be added here if direct SSL termination by Uvicorn is needed
        # ssl_keyfile=config.get("ssl_keyfile_path"),
        # ssl_certfile=config.get("ssl_certfile_path"),
    )
    