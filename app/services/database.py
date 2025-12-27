# app/services/database.py
import asyncpg
import re
import logging
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional
from fastapi import HTTPException, status
from app.config.settings import config
import os
import uuid
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
    stop=stop_after_attempt(10),  # Increased from 3 to 10
    wait=wait_exponential(multiplier=2, min=2, max=30),  # Exponential backoff
    retry=retry_if_exception_type((
        asyncpg.exceptions.PostgresConnectionError,
        ConnectionRefusedError,
        OSError
    )),
    before_sleep=lambda retry_state: logger.warning(
        f"Database connection attempt {retry_state.attempt_number}/10 failed. "
        f"Retrying in {retry_state.next_action.sleep} seconds..."
    )
)
async def init_db_pool(min_connections=config['database'].get('min_pool_connections', 1),
                       max_connections=config['database'].get('max_pool_connections', 10)):
    """Initialize the asyncpg database connection pool with robust retry logic."""
    global connection_pool
    if connection_pool is not None and not connection_pool._closed:
        logger.info("Asyncpg database connection pool already initialized and active.")
        return

    try:
        validate_db_config(config)
        DB_HOST = config['database'].get('host', os.environ.get('DB_HOST', 'localhost'))
        DB_PORT = config['database'].get('port', os.environ.get('DB_PORT', '5432'))
        POSTGRES_DB = config['database'].get('dbname', os.environ.get('POSTGRES_DB', 'appdb'))
        DB_USER = config['database'].get('user', os.environ.get('POSTGRES_USER', 'postgres'))
        POSTGRES_PASSWORD = config['database'].get('password', os.environ.get('POSTGRES_PASSWORD', 'postgres'))

        logger.info(f"Attempting to connect to PostgreSQL at {DB_HOST}:{DB_PORT}/{POSTGRES_DB}")
        
        connection_pool = await asyncpg.create_pool(
            host=DB_HOST,
            port=DB_PORT,
            database=POSTGRES_DB,
            user=DB_USER,
            password=POSTGRES_PASSWORD,
            min_size=min_connections,
            max_size=max_connections,
            init=setup_asyncpg_connection_types,
            timeout=10.0  # Add connection timeout
        )
        logger.info(f"✅ Asyncpg database connection pool initialized successfully (min: {min_connections}, max: {max_connections})")
    except Exception as e:
        logger.error(f"❌ Failed to initialize asyncpg connection pool: {e}", exc_info=True)
        connection_pool = None
        raise
    
async def setup_asyncpg_connection_types(conn: asyncpg.Connection):
    """
    Set up type codecs for an asyncpg connection.
    Configure UUID to use standard Python uuid.UUID instead of asyncpg's custom UUID.
    """
    import uuid
    
    # Override the default UUID codec to return standard Python UUIDs
    await conn.set_type_codec(
        'uuid',
        encoder=str,  # Convert UUID to string for PostgreSQL
        decoder=uuid.UUID,  # Convert PostgreSQL UUID to Python uuid.UUID
        schema='pg_catalog'
    )
    
    logger.info(f"Configured asyncpg connection {conn} to use standard Python UUID objects")

    
async def close_db_pool():
    """Close the asyncpg database connection pool."""
    global connection_pool
    if connection_pool and not connection_pool._closed:
        await connection_pool.close()
        logger.info("Asyncpg database connection pool closed.")
        connection_pool = None

def get_pool() -> asyncpg.Pool:
    if not connection_pool or connection_pool._closed:
        raise RuntimeError("DB pool not initialized")
    return connection_pool

def is_pool_healthy() -> bool:
    """Check if the connection pool is healthy."""
    return connection_pool is not None and not connection_pool._closed

async def check_postgres_health() -> bool:
    try:
        pool = get_pool()
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return True
    except Exception as e:
        logger.warning("Postgres health check failed: %s", e)
        return False

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
        except asyncpg.exceptions.TooManyConnectionsError as e:
            logger.error(f"Pool exhausted: {e}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE, 
                detail="Database busy, too many connections."
            )
        except (asyncpg.exceptions.PostgresConnectionError, ConnectionRefusedError, OSError) as e:
            logger.error(f"Connection error: {e}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE, 
                detail="Cannot connect to database service."
            )
        except Exception as e:
            logger.error(f"Unexpected error acquiring connection: {e}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                detail="Error acquiring database connection."
            )
        finally:
            if conn:
                try:
                    await connection_pool.release(conn)
                except Exception as e:
                    logger.error(f"Error releasing connection: {e}", exc_info=True)

    @asynccontextmanager
    async def transaction(self) -> asyncpg.Connection:
        """Provides a database connection with a transaction."""
        async with self.get_connection() as conn:
            # asyncpg's conn.transaction() handles nesting with savepoints automatically.
            async with conn.transaction():
                yield conn

    async def execute_query(self, query: str, params: Optional[tuple] = None,
                            fetch_one: bool = False, fetch_all: bool = False,
                            return_rowcount: bool = False,
                            connection: Optional[asyncpg.Connection] = None) -> Any:
        """
        Execute a database query using asyncpg.
        If a 'connection' is provided, it uses that (presumably within a transaction).
        Otherwise, it acquires a new connection.
        """
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
                        # Status format examples:
                        # "DELETE 5", "UPDATE 1", "INSERT 0 1", "CREATE TABLE"
                        if not status_str:
                            return 0
                        
                        parts = status_str.split()
                        if len(parts) == 0:
                            return 0
                        
                        # For INSERT: "INSERT oid rows" - take last part
                        # For UPDATE/DELETE: "COMMAND rows" - take last part
                        # For DDL: "CREATE TABLE" etc - return 0
                        last_part = parts[-1]
                        if last_part.isdigit():
                            return int(last_part)
                        else:
                            # DDL command or command without rowcount
                            logger.debug(f"No rowcount in status: '{status_str}' for query: {query[:100]}")
                            return 0
                    except (ValueError, IndexError) as e:
                        logger.warning(f"Could not parse rowcount from status: '{status_str}' for query: {query[:100]}. Error: {e}")
                        return 0
                else:
                    await conn_to_use.execute(query, *params if params else [])
                    return None
            except asyncpg.PostgresError as db_err:
                logger.error(f"Asyncpg database query error: {db_err}. Query: {query[:200]}... Params: {params}", exc_info=True)
                # Specific error handling can be added here, e.g., for UniqueViolationError
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

db_manager = DatabaseManager()
