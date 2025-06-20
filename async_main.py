# async_main.py
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from contextlib import asynccontextmanager
import logging
import signal
import uvicorn
from async_config import config
from datetime import datetime, timezone

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

from async_database import (
    DatabaseManager,
    init_db_pool as async_init_db_pool,
    close_db_pool as async_close_db_pool,
    connection_pool as asyncpg_connection_pool,
    initialize_database,
    ensure_database_indices as async_ensure_database_indices
)

# Setup logging - matching main.py
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(config.get("log_file_path", "app.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup - simplified to match main.py approach
    logger.info("Application startup sequence initiated (async)...")

    try:
        await async_init_db_pool()
        logger.info("Asyncpg database connection pool initialization requested.")
    except Exception as e:
        logger.critical(f"CRITICAL: Failed to initialize asyncpg pool: {e}", exc_info=True)
        raise RuntimeError("Database pool initialization failed.") from e

    # Simple database initialization like main.py
    try:
        logger.info("Create tables if they don't exist")
        await initialize_database()
        logger.info("Created tables successfully")
    except Exception as e:
        logger.critical(f"CRITICAL: Database schema initialization failed (async): {e}", exc_info=True)
        raise RuntimeError(f"Database schema initialization failed (async): {e}") from e

    try:
        await async_ensure_database_indices()
        logger.info("Database indices ensured (async).")
    except Exception as e:
        logger.error(f"Failed to ensure database indices (async): {e}", exc_info=True)

    try:
        await initialize_stream_manager()
        logger.info("Stream manager initialized and background tasks started.")
    except Exception as e:
        logger.error(f"Failed to initialize StreamManager: {e}", exc_info=True)

    logger.info("Application startup complete (async).")
    yield
    
    # Shutdown sequence
    logger.info("Application shutdown sequence initiated (async)...")

    if stream_manager:
        try:
            await stream_manager.shutdown()
            logger.info("Stream manager shutdown complete.")
        except Exception as e:
            logger.error(f"Error during StreamManager shutdown: {e}", exc_info=True)

    if asyncpg_connection_pool and not asyncpg_connection_pool._closed:
        try:
            await async_close_db_pool()
            logger.info("Asyncpg database connection pool closed.")
        except Exception as e:
            logger.error(f"Error closing asyncpg database connection pool: {e}", exc_info=True)

    logger.info("Application shutdown complete (async).")

from starlette.middleware.base import BaseHTTPMiddleware

class CacheControlMiddleware(BaseHTTPMiddleware):
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

# Add CORS middleware - matching main.py's configuration
origins = ["*"]  # Matching main.py's origins configuration
allowed_hosts = ["*"]  # Matching main.py's allowed_hosts configuration

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=config.get("cors_allow_credentials", True),
    allow_methods=config.get("cors_allow_methods", ["*"]),
    allow_headers=config.get("cors_allow_headers", ["*"]),
    expose_headers=config.get("cors_expose_headers", ["X-Request-ID"])
)

# Add trusted hosts middleware - matching main.py
app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=allowed_hosts
)

# Add compression middleware - matching main.py
app.add_middleware(GZipMiddleware, minimum_size=1000)

# Add cache control middleware - matching main.py
app.add_middleware(CacheControlMiddleware)

@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response = await call_next(request)
    is_secure_scheme = request.url.scheme == "https"
    x_forwarded_proto = request.headers.get("x-forwarded-proto")
    is_behind_secure_proxy = x_forwarded_proto == "https"

    if is_secure_scheme or is_behind_secure_proxy or config.get("force_hsts", False):
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains; preload"

    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"

    if "server" in response.headers:
        del response.headers["server"]
    return response

# Include routers - matching main.py
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

# Signal handling - matching main.py
def signal_handler(signum, frame):
    logger.info(f"Signal {signal.Signals(signum).name} received. Initiating graceful shutdown via Uvicorn.")

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

@app.get("/health", tags=["System"])
async def health_check():
    """API health check endpoint - matching main.py"""
    db_healthy = False
    conn_info = "DB pool not checked or inactive"

    if asyncpg_connection_pool and not asyncpg_connection_pool._closed:
        try:
            async with asyncpg_connection_pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            db_healthy = True
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
    server_port = config.get("server_port", 8000)
    reload_app = config.get("debug_reload", False)

    logger.info(f"Starting Uvicorn server on {server_host}:{server_port} with reload: {reload_app}")
    uvicorn.run(
        "async_main:app",
        host=server_host,
        port=server_port,
    )
