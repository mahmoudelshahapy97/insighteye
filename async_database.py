# async_database.py

import asyncio
import asyncpg
import logging
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional, List
from fastapi import HTTPException, status
from async_config import config
import os
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger(__name__)

# Database connection pool for asyncpg
connection_pool: Optional[asyncpg.Pool] = None

def validate_db_config(config: Dict[str, Any]) -> None:
    """Validate database configuration parameters."""
    required_keys = ['host', 'port', 'dbname', 'user', 'password']
    for key in required_keys:
        if key not in config['database'] and not os.environ.get(f"DB_{key.upper()}"):
            raise ValueError(f"Missing database configuration for '{key}'")
    try:
        port = config['database'].get('port', os.environ.get('DB_PORT', '5432'))
        int(port)  # Ensure port is a valid integer
    except ValueError:
        raise ValueError("Database port must be a valid integer")

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type(asyncpg.exceptions.PostgresConnectionError),
    before_sleep=lambda retry_state: logger.info(f"Retrying database pool initialization (attempt {retry_state.attempt_number})...")
)
async def init_db_pool(min_connections=config['database'].get('min_pool_connections', 1),
                       max_connections=config['database'].get('max_pool_connections', 10)):
    """Initialize the asyncpg database connection pool."""
    global connection_pool
    if connection_pool is not None and not connection_pool._closed:
        logger.info("Asyncpg database connection pool already initialized and active.")
        return

    try:
        validate_db_config(config)
        DB_HOST = config['database'].get('host', os.environ.get('DB_HOST', 'localhost'))
        DB_PORT = config['database'].get('port', os.environ.get('DB_PORT', '5432'))
        DB_NAME = config['database'].get('dbname', os.environ.get('DB_NAME', 'appdb'))
        DB_USER = config['database'].get('user', os.environ.get('DB_USER', 'postgres'))
        DB_PASSWORD = config['database'].get('password', os.environ.get('DB_PASSWORD', 'postgres'))

        connection_pool = await asyncpg.create_pool(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            min_size=min_connections,
            max_size=max_connections,
        )
        logger.info(f"Asyncpg database connection pool initialized (min: {min_connections}, max: {max_connections})")
    except Exception as e:
        logger.error(f"Failed to initialize asyncpg connection pool: {e}", exc_info=True)
        connection_pool = None
        raise

async def close_db_pool():
    """Close the asyncpg database connection pool."""
    global connection_pool
    if connection_pool and not connection_pool._closed:
        await connection_pool.close()
        logger.info("Asyncpg database connection pool closed.")
        connection_pool = None

class DatabaseManager:
    """Manages async database connections and queries with connection pooling."""

    def __init__(self):
        pass

    @asynccontextmanager
    async def get_connection(self) -> asyncpg.Connection:
        """Acquire a connection from the pool."""
        if connection_pool is None or connection_pool._closed:
            logger.error("Asyncpg connection pool is not initialized or closed. Attempting to re-initialize.")
            try:
                await init_db_pool()
            except Exception as e:
                logger.critical(f"Failed to re-initialize connection pool during get_connection: {e}", exc_info=True)
                raise HTTPException(status_code=503, detail="Database service critically unavailable: Pool re-initialization failed.")

            if connection_pool is None or connection_pool._closed:
                logger.critical("Connection pool remains uninitialized after attempt.")
                raise HTTPException(status_code=503, detail="Database service unavailable: Pool initialization failed.")

        conn: Optional[asyncpg.Connection] = None
        try:
            conn = await connection_pool.acquire()
            yield conn
        except Exception as e:
            logger.error(f"Error acquiring connection from asyncpg pool: {e}", exc_info=True)
            if isinstance(e, asyncpg.exceptions.TooManyConnectionsError):
                raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Database busy, too many connections.")
            elif isinstance(e, (asyncpg.exceptions.PostgresConnectionError, ConnectionRefusedError, OSError)):
                raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Cannot connect to database service.")
            else:
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error acquiring database connection.")
        finally:
            if conn:
                await connection_pool.release(conn)

    @asynccontextmanager
    async def transaction(self) -> asyncpg.Connection:
        """Provides a database connection with a transaction."""
        async with self.get_connection() as conn:
            async with conn.transaction():
                yield conn

    async def execute_query(self, query: str, params: Optional[tuple] = None,
                            fetch_one: bool = False, fetch_all: bool = False,
                            return_rowcount: bool = False,
                            connection: Optional[asyncpg.Connection] = None) -> Any:
        """Execute a database query using asyncpg."""
        async def _execute(conn_to_use: asyncpg.Connection):
            try:
                if fetch_one:
                    row = await conn_to_use.fetchrow(query, *params if params else [])
                    return dict(row) if row else None
                elif fetch_all:
                    rows = await conn_to_use.fetch(query, *params if params else [])
                    return [dict(row) for row in rows]
                elif return_rowcount:
                    status_str = await conn_to_use.execute(query, *params if params else [])
                    try:
                        return int(status_str.split()[-1]) if status_str and status_str.split()[-1].isdigit() else 0
                    except (ValueError, IndexError):
                        logger.warning(f"Could not parse rowcount from status: '{status_str}' for query: {query[:100]}")
                        return 0
                else:
                    await conn_to_use.execute(query, *params if params else [])
                    return None
            except asyncpg.PostgresError as db_err:
                logger.error(f"Asyncpg database query error: {db_err}. Query: {query[:200]}... Params: {params}", exc_info=True)
                if isinstance(db_err, asyncpg.exceptions.UniqueViolationError):
                    raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Database constraint violation: {db_err.detail or db_err.message}")
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"A database error occurred: {db_err}")
            except Exception as e:
                logger.error(f"Unexpected error during async database query: {e}. Query: {query[:200]}... Params: {params}", exc_info=True)
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred while processing your request.")

        if connection:
            return await _execute(connection)
        else:
            async with self.get_connection() as conn:
                return await _execute(conn)

# Simple database functions to match main.py style
async def get_db_connection():
    """Get a database connection - async version of main.py function."""
    if connection_pool is None or connection_pool._closed:
        await init_db_pool()
    return await connection_pool.acquire()

async def release_db_connection(conn):
    """Release a database connection - async version of main.py function."""
    if connection_pool:
        await connection_pool.release(conn)

async def execute_db_query(query, params=None, fetch_one=False, fetch_all=False, return_rowcount=False):
    """Execute a database query with error handling - async version of main.py function."""
    conn = await get_db_connection()
    result = None
    
    try:
        if fetch_one:
            row = await conn.fetchrow(query, *(params or ()))
            result = dict(row) if row else None
        elif fetch_all:
            rows = await conn.fetch(query, *(params or ()))
            result = [dict(row) for row in rows]
        elif return_rowcount:
            status_str = await conn.execute(query, *(params or ()))
            result = int(status_str.split()[-1]) if status_str and status_str.split()[-1].isdigit() else 0
        else:
            await conn.execute(query, *(params or ()))
            
        return result
    except Exception as e:
        logger.error(f"Database error: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        await release_db_connection(conn)

async def execute_db_transaction(queries_and_params):
    """Execute multiple queries in a single transaction - async version."""
    conn = await get_db_connection()
    results = []
    
    try:
        async with conn.transaction():
            for query, params in queries_and_params:
                if params:
                    result = await conn.execute(query, *params)
                else:
                    result = await conn.execute(query)
                results.append(result)
        
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Transaction error: {e}")
    finally:
        await release_db_connection(conn)

async def ensure_database_indices():
    """Ensure all necessary database indices exist - async version."""
    indices = [
        "CREATE INDEX IF NOT EXISTS idx_video_stream_user_id ON video_stream(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_video_stream_streaming ON video_stream(is_streaming)",
        "CREATE INDEX IF NOT EXISTS idx_video_stream_user_streaming ON video_stream(user_id, is_streaming)",
        "CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)",
        "CREATE INDEX IF NOT EXISTS idx_logs_user_id ON logs(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_logs_action_type ON logs(action_type)",
        "CREATE INDEX IF NOT EXISTS idx_workspaces_name ON workspaces(name)",
        "CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)",
        "CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_sessions_expires_at ON sessions(expires_at)",
        "CREATE INDEX IF NOT EXISTS idx_user_tokens_user_id ON user_tokens(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_user_tokens_is_active ON user_tokens(is_active)",
        "CREATE INDEX IF NOT EXISTS idx_workspace_members_workspace_id ON workspace_members(workspace_id)",
        "CREATE INDEX IF NOT EXISTS idx_workspace_members_user_id ON workspace_members(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_token_blacklist_user_id ON token_blacklist(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_token_blacklist_expires_at ON token_blacklist(expires_at)",
        "CREATE INDEX IF NOT EXISTS idx_notifications_user_id ON notifications(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_security_events_user_id ON security_events(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_security_events_created_at ON security_events(created_at)",
    ]
    
    conn = await get_db_connection()
    
    try:
        async with conn.transaction():
            for index_stmt in indices:
                await conn.execute(index_stmt)
        logger.info("Database indices have been verified/created (async).")
    except Exception as e:
        logger.error(f"Error creating database indices (async): {e}")
        raise HTTPException(status_code=500, detail=f"Failed to create database indices: {str(e)}")
    finally:
        await release_db_connection(conn)

async def execute_complex_sql_file(file_path):
    """Execute a complex SQL file - async version."""
    try:
        with open(file_path, 'r') as sql_file:
            sql_content = sql_file.read()
        
        conn = await get_db_connection()
        
        try:
            logger.info(f"Executing SQL file: {file_path}")
            await conn.execute(sql_content)
            logger.info(f"Successfully executed SQL file: {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error executing SQL file: {e}")
            raise HTTPException(status_code=500, detail=f"SQL file execution error: {e}")
        finally:
            await release_db_connection(conn)
            
    except FileNotFoundError:
        logger.error(f"SQL file not found: {file_path}")
        raise HTTPException(status_code=404, detail=f"SQL file not found: {file_path}")
    except Exception as e:
        logger.error(f"Error reading SQL file: {e}")
        raise HTTPException(status_code=500, detail=f"Error reading SQL file: {e}")

async def initialize_database():
    """Initialize the database with the InsightEye schema - async version matching main.py."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    sql_file_path = os.path.join(current_dir, "insighteye-query_v1.2.sql")
    
    try:
        await execute_complex_sql_file(sql_file_path)
        logger.info("Database schema successfully initialized (async)")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize database schema (async): {e}")
        return False

def validate_video_source(source: str) -> str:
    """Validates if a video source is properly formatted - same as main.py."""
    source = source.replace('\\', '/')
    
    if not source.startswith(('http://', 'https://', 'rtsp://')) and not source.lower() == "local":
        if not source.startswith("/"):
            if not (len(source) > 2 and source[1:3] == ":/"):
                raise ValueError("Source must start with 'http://' or 'https://' or 'rtsp://' or 'local' or local file path with / or with C:/")
    
    if source.lower() != "local" and not source.startswith(('http://', 'https://', 'rtsp://')):
        video_extensions = ['.mp4', '.avi', '.mov', '.mkv', '.wmv', '.flv', '.webm', '.m4v']
        if not any(source.lower().endswith(ext) for ext in video_extensions):
            raise ValueError("Local source must be a video file with extension: " + ", ".join(video_extensions))
    
    return source
    