# async_main.py
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging
import signal
import uvicorn
from async_config import config
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import os
import asyncpg
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
    safe_initialize_database,
    ensure_database_indices as async_ensure_database_indices,
    check_existing_tables, 
    check_table_data_counts,
    diagnose_database_state, 
    validate_schema_structure, 
    check_for_backup_tables, 
    restore_from_backup_table,
    validate_db_config  
)

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

DISABLE_AUTO_INIT = os.environ.get('DISABLE_AUTO_INIT', 'false').lower() == 'true'

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Application startup sequence initiated (async)...")

    try:
        await async_init_db_pool()
        logger.info("Asyncpg database connection pool initialization requested.")
    except Exception as e:
        logger.critical(f"CRITICAL: Failed to initialize asyncpg pool: {e}", exc_info=True)
        raise RuntimeError("Database pool initialization failed.") from e

    # ENHANCED SAFETY: Comprehensive database state check
    try:
        logger.info("Performing comprehensive database state analysis...")
        
        # Get comprehensive diagnosis
        diagnosis = await diagnose_database_state()
        existing_tables = diagnosis.get('existing_core_tables', [])
        missing_tables = diagnosis.get('missing_core_tables', [])
        tables_with_data = diagnosis.get('tables_with_data', [])
        total_records = diagnosis.get('total_records', 0)
        
        logger.info(f"Database diagnosis complete:")
        logger.info(f"  - Existing core tables: {len(existing_tables)}")
        logger.info(f"  - Missing core tables: {len(missing_tables)}")
        logger.info(f"  - Tables with data: {len(tables_with_data)}")
        logger.info(f"  - Total records: {total_records}")
        
        # SAFETY CHECK: If we have data and missing tables, be extra careful
        if total_records > 0 and len(missing_tables) > 0:
            logger.warning("⚠️  SAFETY WARNING: Database contains data but is missing core tables")
            logger.warning(f"   Data found in: {tables_with_data}")
            logger.warning(f"   Missing tables: {missing_tables}")
            logger.warning("   Using SAFE initialization mode to prevent data loss")
            
            # Create automatic backup before any changes
            try:
                logger.info("Creating automatic safety backup...")
                backup_result = await backup_existing_data()
                logger.info(f"Safety backup created: {backup_result.get('message', 'Unknown result')}")
            except Exception as backup_error:
                logger.error(f"Failed to create safety backup: {backup_error}")
                # Continue anyway, but with extra caution
        
        # Determine initialization strategy
        if len(missing_tables) == 0:
            logger.info("✅ Database schema is complete - no initialization needed")
        else:
            logger.info(f"🔧 Database initialization needed for missing tables: {missing_tables}")
            
            # Use safe initialization
            logger.info("Attempting SAFE database schema initialization...")
            if await safe_initialize_database(force_recreate=False):
                logger.info("✅ Database schema initialized successfully with data preservation")
            else:
                logger.error("❌ Database schema initialization failed")
                raise RuntimeError("Database schema initialization incomplete")
        
        # Verify final state
        final_diagnosis = await diagnose_database_state()
        final_missing = final_diagnosis.get('missing_core_tables', [])
        final_records = final_diagnosis.get('total_records', 0)
        
        if len(final_missing) > 0:
            logger.error(f"❌ Initialization complete but still missing tables: {final_missing}")
            raise RuntimeError("Database schema initialization incomplete")
        
        # Data loss check
        if total_records > 0 and final_records < total_records:
            logger.critical(f"🚨 DATA LOSS DETECTED: Records before={total_records}, after={final_records}")
            logger.critical("This should not happen with safe initialization!")
            raise RuntimeError("Data loss detected during initialization")
        elif final_records > total_records:
            logger.info(f"✅ Data preserved and possibly expanded: {total_records} → {final_records} records")
        elif total_records > 0:
            logger.info(f"✅ Data preserved: {total_records} records maintained")
        
        logger.info("Database schema initialized/verified successfully (async).")
        
    except Exception as e:
        logger.critical(f"CRITICAL: Database schema initialization failed (async): {e}", exc_info=True)
        raise RuntimeError(f"Database schema initialization failed (async): {e}") from e

    # Continue with indices and other initialization...
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

    # APScheduler startup
    try:
        scheduler.add_job(
            cleanup_expired_data,
            trigger=CronTrigger(hour="*/1"),
            id="cleanup_expired_data_job",
            replace_existing=True
        )
        scheduler.start()
        logger.info("APScheduler started with cleanup_expired_data scheduled.")
    except Exception as e:
        logger.error(f"Failed to start APScheduler: {e}", exc_info=True)

    logger.info("Application startup complete (async).")
    yield
    
    # Shutdown sequence remains the same...
    logger.info("Application shutdown sequence initiated (async)...")

    if scheduler.running:
        try:
            scheduler.shutdown()
            logger.info("APScheduler shut down successfully.")
        except Exception as e:
            logger.error(f"Error during APScheduler shutdown: {e}", exc_info=True)

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

async def emergency_safe_startup():
    """
    Ultra-safe startup mode that only connects to database without any schema changes.
    Used when normal initialization fails.
    """
    logger.warning("🚨 EMERGENCY SAFE STARTUP MODE ACTIVATED")
    logger.warning("   • No schema initialization will be performed")
    logger.warning("   • Only basic database connectivity will be tested")
    logger.warning("   • Set DISABLE_AUTO_INIT=false to return to normal mode")
    
    try:
        # Only initialize the connection pool
        await async_init_db_pool()
        logger.info("✅ Database connection pool initialized in emergency safe mode")
        
        # Test basic connectivity without changing anything
        if asyncpg_connection_pool and not asyncpg_connection_pool._closed:
            async with asyncpg_connection_pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            logger.info("✅ Database connectivity verified")
        
        # Get read-only diagnosis
        try:
            diagnosis = await diagnose_database_state()
            if not diagnosis.get('error'):
                total_records = diagnosis.get('total_records', 0)
                existing_tables = len(diagnosis.get('existing_core_tables', []))
                missing_tables = len(diagnosis.get('missing_core_tables', []))
                
                logger.info(f"📊 Database state (read-only): {total_records} records, {existing_tables} core tables present, {missing_tables} missing")
                
                if diagnosis.get('backup_tables'):
                    logger.info(f"💾 Found {len(diagnosis['backup_tables'])} backup tables for potential recovery")
            else:
                logger.warning(f"⚠️  Could not diagnose database state: {diagnosis['error']}")
        except Exception as diag_error:
            logger.warning(f"⚠️  Database diagnosis failed in safe mode: {diag_error}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Even emergency safe startup failed: {e}", exc_info=True)
        return False

# Additional safety function for automatic backups
async def backup_existing_data():
    """Create backup tables for all existing data (used internally for safety)."""
    existing_tables = await check_existing_tables()
    table_counts = await check_table_data_counts()
    
    # Only backup tables that have data
    tables_to_backup = [table for table, count in table_counts.items() if count > 0]
    
    if not tables_to_backup:
        return {"message": "No tables with data found to backup", "backed_up_tables": []}
    
    validate_db_config(config)
    DB_HOST = config['database'].get('host', 'localhost')
    DB_PORT = config['database'].get('port', '5432')
    DB_NAME = config['database'].get('dbname', 'appdb')
    DB_USER = config['database'].get('user', 'postgres')
    DB_PASSWORD = config['database'].get('password', 'postgres')

    conn = await asyncpg.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
    
    backed_up_tables = []
    backup_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    
    try:
        async with conn.transaction():
            for table in tables_to_backup:
                backup_table_name = f"{table}_safety_backup_{backup_timestamp}"
                
                try:
                    # Create backup table
                    backup_query = f'CREATE TABLE "{backup_table_name}" AS SELECT * FROM "{table}"'
                    await conn.execute(backup_query)
                    
                    # Verify backup
                    backup_count = await conn.fetchval(f'SELECT COUNT(*) FROM "{backup_table_name}"')
                    original_count = table_counts[table]
                    
                    if backup_count == original_count:
                        backed_up_tables.append({
                            "original_table": table,
                            "backup_table": backup_table_name,
                            "record_count": backup_count
                        })
                        logger.info(f"Safety backup: {backup_count} records from '{table}' → '{backup_table_name}'")
                    else:
                        logger.warning(f"Backup verification failed for '{table}': expected {original_count}, got {backup_count}")
                        
                except Exception as table_error:
                    logger.warning(f"Could not backup table '{table}': {table_error}")
                    # Continue with other tables
                    
    finally:
        await conn.close()
    
    return {
        "success": True,
        "message": f"Safety backup created for {len(backed_up_tables)} tables",
        "backup_timestamp": backup_timestamp,
        "backed_up_tables": backed_up_tables
    }


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

@app.get("/admin/database/diagnose", tags=["Admin"])
async def diagnose_database():
    """Comprehensive database diagnosis to understand schema state."""
    try:
        diagnosis = await diagnose_database_state()
        return diagnosis
    except Exception as e:
        logger.error(f"Error during database diagnosis: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Database diagnosis failed: {str(e)}")

@app.get("/admin/database/status", tags=["Admin"])
async def database_status():
    """Get current database status and table information (enhanced version)."""
    try:
        existing_tables = await check_existing_tables()
        table_counts = await check_table_data_counts()
        schema_validation = await validate_schema_structure()
        
        core_tables = [
            "users", "workspaces", "user_accounts", "workspace_members",
            "param_stream", "video_stream", "user_tokens", "sessions",
            "notifications", "otps", "token_blacklist", "logs", "security_events"
        ]
        missing_core_tables = [table for table in core_tables if table not in existing_tables]
        invalid_tables = [table for table, is_valid in schema_validation.items() if not is_valid]
        
        return {
            "existing_tables": existing_tables,
            "table_counts": table_counts,
            "schema_validation": schema_validation,
            "total_records": sum(count for count in table_counts.values() if count > 0),
            "empty_tables": [table for table, count in table_counts.items() if count == 0],
            "tables_with_data": [table for table, count in table_counts.items() if count > 0],
            "core_tables": core_tables,
            "missing_core_tables": missing_core_tables,
            "invalid_tables": invalid_tables,
            "schema_complete": len(missing_core_tables) == 0,
            "schema_valid": len(invalid_tables) == 0,
            "initialization_needed": len(missing_core_tables) > 0 or len(invalid_tables) > 0
        }
    except Exception as e:
        logger.error(f"Error getting database status: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error getting database status: {str(e)}")

@app.post("/admin/database/safe-initialize", tags=["Admin"])
async def safe_database_initialization(force_recreate: bool = False):
    """
    Safely initialize database schema using CREATE TABLE IF NOT EXISTS.
    This should not cause data loss.
    """
    try:
        logger.info(f"Safe database initialization requested with force_recreate={force_recreate}")
        
        # Get current state before initialization
        before_state = await diagnose_database_state()
        logger.info(f"Database state before initialization: {len(before_state.get('tables_with_data', []))} tables with data")
        
        result = await safe_initialize_database(force_recreate=force_recreate)
        
        # Get state after initialization
        after_state = await diagnose_database_state()
        
        if result:
            return {
                "success": True,
                "message": "Database schema initialized safely",
                "before_state": before_state,
                "after_state": after_state,
                "data_preserved": before_state.get('total_records', 0) <= after_state.get('total_records', 0)
            }
        else:
            return {
                "success": False,
                "message": "Database initialization failed - check logs for details",
                "before_state": before_state,
                "after_state": after_state
            }
    except Exception as e:
        logger.error(f"Error during safe database initialization: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Safe database initialization failed: {str(e)}")

@app.post("/admin/database/backup-data", tags=["Admin"])
async def backup_existing_data_():
    """Create backup tables for all existing data before any schema changes."""
    try:
        existing_tables = await check_existing_tables()
        table_counts = await check_table_data_counts()
        
        # Only backup tables that have data
        tables_to_backup = [table for table, count in table_counts.items() if count > 0]
        
        if not tables_to_backup:
            return {"message": "No tables with data found to backup", "backed_up_tables": []}
        
        validate_db_config(config)
        DB_HOST = config['database'].get('host', 'localhost')
        DB_PORT = config['database'].get('port', '5432')
        DB_NAME = config['database'].get('dbname', 'appdb')
        DB_USER = config['database'].get('user', 'postgres')
        DB_PASSWORD = config['database'].get('password', 'postgres')

        conn = await asyncpg.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
        
        backed_up_tables = []
        backup_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        
        try:
            async with conn.transaction():
                for table in tables_to_backup:
                    backup_table_name = f"{table}_backup_{backup_timestamp}"
                    
                    # Create backup table
                    backup_query = f'CREATE TABLE "{backup_table_name}" AS SELECT * FROM "{table}"'
                    await conn.execute(backup_query)
                    
                    # Verify backup
                    backup_count = await conn.fetchval(f'SELECT COUNT(*) FROM "{backup_table_name}"')
                    original_count = table_counts[table]
                    
                    if backup_count == original_count:
                        backed_up_tables.append({
                            "original_table": table,
                            "backup_table": backup_table_name,
                            "record_count": backup_count
                        })
                        logger.info(f"Successfully backed up {backup_count} records from '{table}' to '{backup_table_name}'")
                    else:
                        logger.error(f"Backup verification failed for '{table}': expected {original_count}, got {backup_count}")
                        raise Exception(f"Backup verification failed for table '{table}'")
        finally:
            await conn.close()
        
        return {
            "success": True,
            "message": f"Successfully backed up {len(backed_up_tables)} tables",
            "backup_timestamp": backup_timestamp,
            "backed_up_tables": backed_up_tables
        }
        
    except Exception as e:
        logger.error(f"Error backing up data: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Backup operation failed: {str(e)}")

@app.post("/admin/database/restore/{backup_table}/{target_table}", tags=["Admin"])
async def restore_backup_data(backup_table: str, target_table: str):
    """Restore data from a backup table to the main table."""
    try:
        success = await restore_from_backup_table(backup_table, target_table)
        if success:
            return {"success": True, "message": f"Data restored from '{backup_table}' to '{target_table}'"}
        else:
            return {"success": False, "message": "Restore operation failed - check logs for details"}
    except Exception as e:
        logger.error(f"Error during restore operation: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Restore operation failed: {str(e)}")

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
