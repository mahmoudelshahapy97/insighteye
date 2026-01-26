# app/services/database.py
import asyncpg
import re
import logging
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional, List
from fastapi import HTTPException, status
from app.config.settings import config
import os
import uuid
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import asyncio
from datetime import datetime

logger = logging.getLogger(__name__)

# Database connection pool for asyncpg
connection_pool: Optional[asyncpg.Pool] = None

# Pool statistics
pool_stats = {
    'total_connections': 0,
    'active_connections': 0,
    'idle_connections': 0,
    'total_queries': 0,
    'failed_queries': 0,
    'last_error': None,
    'last_health_check': None
}

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
async def init_db_pool(
    min_connections=None,
    max_connections=None
):
    """
    Initialize the asyncpg database connection pool with robust retry logic.
    Optimized for remote database connections with high concurrency.
    """
    global connection_pool
    if connection_pool is not None and not connection_pool._closed:
        logger.info("Asyncpg database connection pool already initialized and active.")
        return

    try:
        
        # Get configuration with optimized defaults
        DB_HOST = config.db_host
        DB_PORT = int(config.db_port)
        POSTGRES_DB = config.postgres_db
        DB_USER = config.postgres_user
        POSTGRES_PASSWORD = config.postgres_password
        
        # Optimized pool settings for remote database
        min_size = config.db_min_pool_size
        max_size = config.db_max_pool_size
        
        # Connection timeout settings
        timeout = config.db_timeout
        command_timeout = config.db_command_timeout

        logger.info(f"Attempting to connect to PostgreSQL at {DB_HOST}:{DB_PORT}/{POSTGRES_DB}")
        logger.info(f"Pool configuration: min={min_size}, max={max_size}, timeout={timeout}s")
        
        connection_pool = await asyncpg.create_pool(
            host=DB_HOST,
            port=DB_PORT,
            database=POSTGRES_DB,
            user=DB_USER,
            password=POSTGRES_PASSWORD,
            min_size=min_size,
            max_size=max_size,
            max_queries=50000,  # Maximum queries per connection before recycling
            max_inactive_connection_lifetime=1800.0,  # Recycle idle connections after 30 minutes
            timeout=timeout,  # Connection acquire timeout
            command_timeout=command_timeout,  # Query execution timeout
            init=setup_asyncpg_connection,
            setup=setup_asyncpg_connection_types,
            statement_cache_size=0,  # Disable prepared statements for PgBouncer
            # Server settings for each connection
            server_settings={
                'application_name': 'insighteye_camera_streams',
                # 'jit': 'off',  # Disable JIT for faster connection setup
                # 'statement_timeout': '30s', 
            }
        )
        
        # Warm up the pool
        await warmup_pool(min_size)
        
        # Update stats
        pool_stats['total_connections'] = max_size
        pool_stats['last_health_check'] = datetime.utcnow()
        
        logger.info(
            f"✅ Asyncpg database connection pool initialized successfully "
            f"(min: {min_size}, max: {max_size}, timeout: {timeout}s)"
        )
        
    except Exception as e:
        logger.error(f"❌ Failed to initialize asyncpg connection pool: {e}", exc_info=True)
        pool_stats['last_error'] = str(e)
        connection_pool = None
        raise

async def setup_asyncpg_connection(conn: asyncpg.Connection):
    """
    Initial setup for each connection when created.
    Configure connection-level settings for optimal performance.
    """
    try:
        # Set statement timeout (fallback if not set in postgresql.conf)
        await conn.execute("SET statement_timeout = '120s'")
        
        # Set idle transaction timeout
        await conn.execute("SET idle_in_transaction_session_timeout = '300s'")
        
        # # Optimize for streaming queries
        # await conn.execute("SET work_mem = '32MB'")
        
        # Log connection creation
        logger.debug(f"New database connection created: {id(conn)}")
        
    except Exception as e:
        logger.error(f"Error during connection setup: {e}", exc_info=True)
        raise
    
async def setup_asyncpg_connection_types(conn: asyncpg.Connection):
    """
    Set up type codecs for an asyncpg connection.
    Configure UUID to use standard Python uuid.UUID instead of asyncpg's custom UUID.
    """
    try:
        # Override the default UUID codec to return standard Python UUIDs
        await conn.set_type_codec(
            'uuid',
            encoder=str,  # Convert UUID to string for PostgreSQL
            decoder=uuid.UUID,  # Convert PostgreSQL UUID to Python uuid.UUID
            schema='pg_catalog'
        )
        
        logger.debug(f"Configured asyncpg connection {id(conn)} to use standard Python UUID objects")
        
    except Exception as e:
        logger.error(f"Error configuring connection types: {e}", exc_info=True)
        raise

async def warmup_pool(target_connections: int):
    """
    Pre-create connections to warm up the pool.
    This prevents cold start delays when cameras start streaming.
    """
    try:
        logger.info(f"Warming up connection pool with {target_connections} connections...")
        connections = []
        
        for i in range(target_connections):
            try:
                conn = await connection_pool.acquire(timeout=10)
                # Test connection
                await conn.execute("SELECT 1")
                connections.append(conn)
            except Exception as e:
                logger.warning(f"Failed to warm up connection {i+1}/{target_connections}: {e}")
                break
        
        # Release all connections back to pool
        for conn in connections:
            await connection_pool.release(conn)
        
        logger.info(f"Pool warmed up with {len(connections)} connections")
        
    except Exception as e:
        logger.error(f"Error warming up pool: {e}", exc_info=True)
    
async def close_db_pool():
    """Close the asyncpg database connection pool gracefully."""
    global connection_pool
    if connection_pool and not connection_pool._closed:
        try:
            # Wait for all connections to be released (with timeout)
            await asyncio.wait_for(
                connection_pool.close(),
                timeout=30.0
            )
            logger.info("Asyncpg database connection pool closed gracefully.")
        except asyncio.TimeoutError:
            logger.warning("Connection pool close timed out, terminating forcefully")
            connection_pool.terminate()
        except Exception as e:
            logger.error(f"Error closing connection pool: {e}", exc_info=True)
        finally:
            connection_pool = None

def get_pool() -> asyncpg.Pool:
    """Get the connection pool, raise error if not initialized."""
    if not connection_pool or connection_pool._closed:
        raise RuntimeError("DB pool not initialized or closed")
    return connection_pool

def is_pool_healthy() -> bool:
    """Check if the connection pool is healthy."""
    return connection_pool is not None and not connection_pool._closed

async def check_postgres_health() -> bool:
    """
    Check if PostgreSQL is accessible and responsive.
    Updates pool statistics.
    """
    try:
        pool = get_pool()
        start_time = datetime.utcnow()
        
        async with pool.acquire(timeout=5.0) as conn:
            await conn.fetchval("SELECT 1")
        
        # Calculate response time
        response_time = (datetime.utcnow() - start_time).total_seconds()
        
        # Update stats
        pool_stats['last_health_check'] = datetime.utcnow()
        pool_stats['active_connections'] = pool.get_size() - pool.get_idle_size()
        pool_stats['idle_connections'] = pool.get_idle_size()
        
        logger.debug(f"Health check passed (response time: {response_time:.3f}s)")
        return True
        
    except Exception as e:
        logger.warning(f"Postgres health check failed: {e}")
        pool_stats['last_error'] = str(e)
        return False

def get_pool_stats() -> Dict[str, Any]:
    """
    Get current connection pool statistics.
    Useful for monitoring and debugging.
    """
    if not connection_pool or connection_pool._closed:
        return {
            'status': 'closed',
            'error': 'Pool not initialized'
        }
    
    return {
        'status': 'healthy' if is_pool_healthy() else 'unhealthy',
        'size': connection_pool.get_size(),
        'idle': connection_pool.get_idle_size(),
        'active': connection_pool.get_size() - connection_pool.get_idle_size(),
        'max_size': connection_pool.get_max_size(),
        'min_size': connection_pool.get_min_size(),
        'total_queries': pool_stats['total_queries'],
        'failed_queries': pool_stats['failed_queries'],
        'last_health_check': pool_stats['last_health_check'].isoformat() if pool_stats['last_health_check'] else None,
        'last_error': pool_stats['last_error']
    }

class DatabaseManager:
    """Manages async database connections and queries with connection pooling."""

    def __init__(self):
        self._connection_semaphore = asyncio.Semaphore(100)  # Limit concurrent operations

    @asynccontextmanager
    async def get_connection(self) -> asyncpg.Connection:
        """
        Acquire a connection from the pool with timeout and retry logic.
        """
        if connection_pool is None or connection_pool._closed:
            logger.error("Asyncpg connection pool is not initialized or closed. Attempting to re-initialize.")
            try:
                await init_db_pool()
            except Exception as e:
                logger.critical(f"Failed to re-initialize connection pool: {e}", exc_info=True)
                raise HTTPException(
                    status_code=503, 
                    detail="Database service critically unavailable: Pool re-initialization failed."
                )

            if connection_pool is None or connection_pool._closed:
                logger.critical("Connection pool remains uninitialized after attempt.")
                raise HTTPException(
                    status_code=503, 
                    detail="Database service unavailable: Pool initialization failed."
                )

        conn: Optional[asyncpg.Connection] = None
        acquire_start = datetime.utcnow()
        
        try:
            # Use semaphore to limit concurrent connection acquisitions
            async with self._connection_semaphore:
                # Acquire with timeout
                conn = await asyncio.wait_for(
                    connection_pool.acquire(),
                    timeout=30.0
                )
                
                acquire_time = (datetime.utcnow() - acquire_start).total_seconds()
                if acquire_time > 5.0:
                    logger.warning(f"Slow connection acquisition: {acquire_time:.2f}s")
                
                yield conn
                
        except asyncio.TimeoutError:
            logger.error("Connection acquisition timed out after 30s")
            pool_stats['failed_queries'] += 1
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Database connection timeout - service is busy"
            )
            
        except asyncpg.exceptions.TooManyConnectionsError as e:
            logger.error(f"Pool exhausted: {e}", exc_info=True)
            pool_stats['failed_queries'] += 1
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE, 
                detail="Database busy, too many connections."
            )
            
        except (asyncpg.exceptions.PostgresConnectionError, ConnectionRefusedError, OSError) as e:
            logger.error(f"Connection error: {e}", exc_info=True)
            pool_stats['failed_queries'] += 1
            pool_stats['last_error'] = str(e)
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE, 
                detail="Cannot connect to database service."
            )
            
        except Exception as e:
            logger.error(f"Unexpected error acquiring connection: {e}", exc_info=True)
            pool_stats['failed_queries'] += 1
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                detail="Error acquiring database connection."
            )
        except asyncpg.PostgresConnectionError:
            if conn:
                await conn.close()
            raise
            
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

    async def execute_query(
        self, 
        query: str, 
        params: Optional[tuple] = None,
        fetch_one: bool = False, 
        fetch_all: bool = False,
        return_rowcount: bool = False,
        connection: Optional[asyncpg.Connection] = None,
        timeout: Optional[float] = None
    ) -> Any:
        """
        Execute a database query using asyncpg with timeout support.
        """
        async def _execute(conn_to_use: asyncpg.Connection):
            query_start = datetime.utcnow()
            
            try:
                pool_stats['total_queries'] += 1
                
                if fetch_one:
                    row = await conn_to_use.fetchrow(query, *params if params else [], timeout=timeout)
                    return dict(row) if row else None
                    
                elif fetch_all:
                    rows = await conn_to_use.fetch(query, *params if params else [], timeout=timeout)
                    return [dict(row) for row in rows]
                    
                elif return_rowcount:
                    status_str = await conn_to_use.execute(query, *params if params else [], timeout=timeout)
                    
                    try:
                        if not status_str:
                            return 0
                        
                        parts = status_str.split()
                        if len(parts) == 0:
                            return 0
                        
                        last_part = parts[-1]
                        if last_part.isdigit():
                            return int(last_part)
                        else:
                            logger.debug(f"No rowcount in status: '{status_str}' for query: {query[:100]}")
                            return 0
                            
                    except (ValueError, IndexError) as e:
                        logger.warning(f"Could not parse rowcount from status: '{status_str}'. Error: {e}")
                        return 0
                else:
                    await conn_to_use.execute(query, *params if params else [], timeout=timeout)
                    return None
                    
            except asyncio.TimeoutError:
                query_time = (datetime.utcnow() - query_start).total_seconds()
                logger.error(f"Query timeout after {query_time:.2f}s. Query: {query[:200]}")
                pool_stats['failed_queries'] += 1
                raise HTTPException(
                    status_code=status.HTTP_504_GATEWAY_TIMEOUT,
                    detail=f"Database query timeout after {query_time:.1f}s"
                )
                
            except asyncpg.PostgresError as db_err:
                query_time = (datetime.utcnow() - query_start).total_seconds()
                logger.error(
                    f"Database query error after {query_time:.2f}s: {db_err}. "
                    f"Query: {query[:200]}... Params: {params}", 
                    exc_info=True
                )
                pool_stats['failed_queries'] += 1
                pool_stats['last_error'] = str(db_err)
                
                if isinstance(db_err, asyncpg.exceptions.UniqueViolationError):
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT, 
                        detail=f"Database constraint violation: {db_err.detail or db_err.message}"
                    )
                    
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                    detail=f"A database error occurred: {db_err}"
                )
                
            except Exception as e:
                query_time = (datetime.utcnow() - query_start).total_seconds()
                logger.error(
                    f"Unexpected error after {query_time:.2f}s: {e}. "
                    f"Query: {query[:200]}... Params: {params}", 
                    exc_info=True
                )
                pool_stats['failed_queries'] += 1
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                    detail="An unexpected error occurred while processing your request."
                )
            finally:
                query_time = (datetime.utcnow() - query_start).total_seconds()
                if query_time > 5.0:
                    logger.warning(f"Slow query detected: {query_time:.2f}s - {query[:100]}")

        if connection:
            return await _execute(connection)
        else:
            async with self.get_connection() as conn:
                return await _execute(conn)

    async def execute_batch(
        self,
        query: str,
        params_list: List[tuple],
        batch_size: int = 100
    ) -> int:
        """
        Execute a batch of queries efficiently using executemany.
        Returns total number of rows affected.
        """
        if not params_list:
            return 0
            
        total_affected = 0
        
        async with self.get_connection() as conn:
            # Process in batches
            for i in range(0, len(params_list), batch_size):
                batch = params_list[i:i + batch_size]
                
                try:
                    await conn.executemany(query, batch)
                    total_affected += len(batch)
                    logger.debug(f"Batch executed: {len(batch)} rows")
                    
                except Exception as e:
                    logger.error(f"Batch execution failed: {e}", exc_info=True)
                    pool_stats['failed_queries'] += 1
                    raise
        
        return total_affected

# Global database manager instance
db_manager = DatabaseManager()