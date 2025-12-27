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
from app.services.stream_service_3 import stream_manager, initialize_stream_manager

from zoneinfo import ZoneInfo
from datetime import datetime, timezone
from app.services.database import (
    init_db_pool, close_db_pool, connection_pool, 
    get_pool,
    check_postgres_health
)

setup_logging(config.get("log_file_path", "/app/logs/app.log"))
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
        await initialize_stream_manager() 
        logger.info("Stream manager initialized and background tasks started.")
    except Exception as e:
        logger.error(f"Failed to initialize StreamManager: {e}", exc_info=True)


    logger.info("Application startup complete (async).")
    yield
    # Shutdown
    logger.info("Application shutdown sequence initiated (async)...")

    if stream_manager:
        try:
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

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=config.get("cors_allow_credentials", True),
    allow_methods=config.get("cors_allow_methods", ["*"]),
    allow_headers=config.get("cors_allow_headers", ["*"]),
    expose_headers=config.get("cors_expose_headers", ["X-Request-ID"])
)

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


@app.on_event("startup")
async def on_startup():
    try:
        await init_db_pool()  # Ensures retry until DB is ready
    except Exception as e:
        logger.error(f"Startup DB connection failed ❌: {e}")

@app.on_event("shutdown")
async def shutdown_event():
    """Application shutdown"""
    
    logging.info("Shutting down application...")
    await stream_manager.shutdown()
    logging.info("✅ Application shutdown complete")

if __name__ == "__main__":
    server_host = config.get("server_host", "0.0.0.0")
    server_port = config.get("server_port", 8000)
    reload_app = config.get("debug_reload", False) 

    logger.info(f"Starting Uvicorn server on {server_host}:{server_port} with reload: {reload_app}")
    uvicorn.run(
        "main:app", 
        host=server_host,
        port=server_port,
    )
    