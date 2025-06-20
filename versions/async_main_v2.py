# async_main.py - Enhanced with safer database initialization
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
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
from users import router as users_router
from async_otp import router as otp_router
from async_login_user import router as login_router
from text_chat import router as chat_router
from async_stream_one import router as stream_one_router, stream_manager, initialize_stream_manager
from qdrant_chat import router as qdrant_chat_router
from async_workspaces import router as workspace_router
from admin_router import router as admin_utils_router

from datetime import datetime, timezone
from async_database import DatabaseManager # For scheduler task
from async_database import (init_db_pool as async_init_db_pool,
                      close_db_pool as async_close_db_pool,
                      connection_pool as asyncpg_connection_pool,
                      safe_initialize_database as async_safe_initialize_database,  # Use safe version
                      ensure_database_indices as async_ensure_database_indices,
                      check_existing_tables, check_table_data_counts)

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
        # For direct use of asyncpg pool within this function (if DatabaseManager is complex)
        if not asyncpg_connection_pool or asyncpg_connection_pool._closed:
            logger.warning("cleanup_expired_data: DB pool not available or closed.")
            return

        async with asyncpg_connection_pool.acquire() as conn:
            async with conn.transaction():
                # Clean up expired OTPs
                deleted_otps = await conn.execute("DELETE FROM otps WHERE expires_at < CURRENT_TIMESTAMP")
                logger.info(f"Expired OTPs cleaned up: {deleted_otps}")

                # Clean up expired sessions
                deleted_sessions = await conn.execute("DELETE FROM sessions WHERE expires_at < CURRENT_TIMESTAMP")
                logger.info(f"Expired sessions cleaned up: {deleted_sessions}")

                # Clean up expired blacklisted tokens
                deleted_tokens = await conn.execute("DELETE FROM token_blacklist WHERE expires_at < CURRENT_TIMESTAMP")
                logger.info(f"Expired blacklisted tokens cleaned up: {deleted_tokens}")
    except Exception as e:
        logger.error(f"Error during cleanup_expired_data: {e}", exc_info=True)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Application startup sequence initiated (async)...")

    try:
        await async_init_db_pool()
        logger.info("Asyncpg database connection pool initialization requested.")
    except Exception as e:
        logger.critical(f"CRITICAL: Failed to initialize asyncpg pool: {e}", exc_info=True)
        raise RuntimeError("Database pool initialization failed.") from e

    # Enhanced database initialization with safety checks
    try:
        logger.info("Checking existing database state...")
        existing_tables = await check_existing_tables()
        table_counts = await check_table_data_counts()
        
        # Check if we have a complete schema
        core_tables = [
            "users", "workspaces", "user_accounts", "workspace_members",
            "param_stream", "video_stream", "user_tokens", "sessions",
            "notifications", "otps", "token_blacklist", "logs", "security_events"
        ]
        missing_core_tables = [table for table in core_tables if table not in existing_tables]
        
        if existing_tables:
            logger.info(f"Found existing tables: {existing_tables}")
            total_records = sum(count for count in table_counts.values() if count > 0)
            if total_records > 0:
                logger.info(f"Database contains {total_records} records across {len([t for t, c in table_counts.items() if c > 0])} tables")
                if missing_core_tables:
                    logger.warning(f"Missing core tables: {missing_core_tables}")
                    logger.info("Will proceed with schema initialization to create missing tables")
                else:
                    logger.info("All core tables present - skipping schema recreation to preserve existing data")
            else:
                logger.info("Tables exist but are empty - safe to reinitialize if needed")
        else:
            logger.info("No existing tables found - proceeding with schema initialization")
        
        # Force initialization if core tables are missing
        force_init = len(missing_core_tables) > 0
        if force_init:
            logger.info("Core tables missing - forcing schema initialization")
        
        logger.info("Attempting to initialize database schema (async)...")
        if await async_safe_initialize_database(force_recreate=force_init):#
            logger.info("Database schema initialized/verified successfully (async).")
        else:
            logger.error("Database schema initialization failed - missing core tables")
            raise RuntimeError("Database schema initialization incomplete")
    except Exception as e:
        logger.critical(f"CRITICAL: Database schema initialization failed (async): {e}", exc_info=True)
        raise RuntimeError(f"Database schema initialization failed (async): {e}") from e

    try:
        await async_ensure_database_indices()
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
    root_path=config.get("fastapi_root_path", "/insighteye"),
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

# Add cache control middleware - This was active in main.py
app.add_middleware(CacheControlMiddleware)

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

# Add database management endpoints for admin use
@app.get("/admin/database/status", tags=["Admin"])
async def database_status():
    """Get current database status and table information"""
    try:
        existing_tables = await check_existing_tables()
        table_counts = await check_table_data_counts()
        
        # Check completeness
        core_tables = [
            "users", "workspaces", "user_accounts", "workspace_members",
            "param_stream", "video_stream", "user_tokens", "sessions",
            "notifications", "otps", "token_blacklist", "logs", "security_events"
        ]
        missing_core_tables = [table for table in core_tables if table not in existing_tables]
        
        return {
            "existing_tables": existing_tables,
            "table_counts": table_counts,
            "total_records": sum(count for count in table_counts.values() if count > 0),
            "empty_tables": [table for table, count in table_counts.items() if count == 0],
            "tables_with_data": [table for table, count in table_counts.items() if count > 0],
            "core_tables": core_tables,
            "missing_core_tables": missing_core_tables,
            "schema_complete": len(missing_core_tables) == 0
        }
    except Exception as e:
        logger.error(f"Error getting database status: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error getting database status: {str(e)}")

@app.post("/admin/database/initialize", tags=["Admin"])
async def force_database_initialization(force_recreate: bool = False):
    """Force database schema initialization (admin only)"""
    try:
        logger.info(f"Manual database initialization requested with force_recreate={force_recreate}")
        result = await async_safe_initialize_database(force_recreate=force_recreate)
        
        if result:
            # Re-check status after initialization
            existing_tables = await check_existing_tables()
            table_counts = await check_table_data_counts()
            return {
                "success": True,
                "message": "Database schema initialized successfully",
                "existing_tables": existing_tables,
                "table_counts": table_counts
            }
        else:
            return {
                "success": False,
                "message": "Database initialization failed - check logs for details"
            }
    except Exception as e:
        logger.error(f"Error during manual database initialization: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Database initialization failed: {str(e)}")

if __name__ == "__main__":
    server_host = config.get("server_host", "0.0.0.0")
    server_port = config.get("server_port", 8000)
    reload_app = config.get("debug_reload", False) # Keep variable for consistency, though uvicorn.run below doesn't use it directly

    logger.info(f"Starting Uvicorn server on {server_host}:{server_port} with reload: {reload_app}")
    uvicorn.run(
        "async_main:app", # <<< CRITICAL FIX: Changed from "main:app"
        host=server_host,
        port=server_port,
    )
