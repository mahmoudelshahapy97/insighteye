# async_database.py

import asyncpg
import logging
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional, List
from fastapi import HTTPException, status
from async_config import config
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
            init=setup_asyncpg_connection_types
        )
        logger.info(f"Asyncpg database connection pool initialized (min: {min_connections}, max: {max_connections})")
    except Exception as e:
        logger.error(f"Failed to initialize asyncpg connection pool: {e}", exc_info=True)
        connection_pool = None
        raise

async def setup_asyncpg_connection_types(conn: asyncpg.Connection):
    """
    Set up type codecs for an asyncpg connection.
    Relies on asyncpg's built-in support for UUID and other common types.
    """
    logger.info(
        f"For asyncpg connection {conn}: Relying on default built-in codecs for UUID. "
        "Python uuid.UUID objects will be automatically handled for PostgreSQL UUID columns."
    )
    logger.debug(f"Asyncpg connection {conn} type codecs setup complete (relying on defaults for common types).")

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
            # Check if the error is due to pool exhaustion or other transient issues
            if isinstance(e, asyncpg.exceptions.TooManyConnectionsError):
                raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Database busy, too many connections.")
            elif isinstance(e, (asyncpg.exceptions.PostgresConnectionError, ConnectionRefusedError, OSError)): # OSError for dns issues
                raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Cannot connect to database service.")
            else: # Other unexpected errors
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error acquiring database connection.")
        finally:
            if conn:
                await connection_pool.release(conn)

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
                        # Handles "COMMAND rows" format, e.g., "DELETE 5", "UPDATE 1"
                        # For "INSERT oid rows", it takes the last part (rows).
                        # For DDL commands like "CREATE TABLE", it correctly returns 0.
                        return int(status_str.split()[-1]) if status_str and status_str.split()[-1].isdigit() else 0
                    except (ValueError, IndexError):
                        logger.warning(f"Could not parse rowcount from status: '{status_str}' for query: {query[:100]}")
                        return 0 # Default for non-DML or unparseable status
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

# ================== CORRECTED FUNCTION BELOW ==================

async def initialize_database():
    """Initialize the database with the InsightEye schema using asyncpg."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    sql_file_path = os.path.join(current_dir, "insighteye-query_v1.2.sql")

    validate_db_config(config)
    DB_HOST = config['database'].get('host', 'localhost')
    DB_PORT = config['database'].get('port', '5432')
    DB_NAME = config['database'].get('dbname', 'appdb')
    DB_USER = config['database'].get('user', 'postgres')
    DB_PASSWORD = config['database'].get('password', 'postgres')

    conn: Optional[asyncpg.Connection] = None
    try:
        with open(sql_file_path, 'r') as sql_file:
            sql_content = sql_file.read()

        script_parts = sql_content.split('BEGIN;', 1)
        if len(script_parts) != 2:
            raise RuntimeError(f"The SQL script at {sql_file_path} does not have the expected 'BEGIN;' separator.")

        # The part before "BEGIN;" (e.g., CREATE EXTENSION)
        pre_transaction_sql = script_parts[0].strip()
        # The part from "BEGIN;" onwards
        main_transaction_sql = ('BEGIN;' + script_parts[1]).strip()


        conn = await asyncpg.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
        await setup_asyncpg_connection_types(conn) # Ensure types are set up on this direct connection too

        if pre_transaction_sql:
            logger.info("Executing pre-transaction statements (e.g., CREATE EXTENSION)...")
            await conn.execute(pre_transaction_sql)
            logger.info("Pre-transaction statements executed successfully.")

        logger.info(f"Executing main schema transaction from: {sql_file_path}")
        await conn.execute(main_transaction_sql)
        logger.info("Database schema successfully initialized/verified with asyncpg.")
        return True
    except FileNotFoundError:
        logger.error(f"SQL schema file not found: {sql_file_path}")
        raise
    except asyncpg.PostgresError as e:
        logger.error(f"Error initializing database schema with asyncpg: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unexpected error during schema initialization with asyncpg: {e}", exc_info=True)
        raise
    finally:
        if conn and not conn.is_closed():
            await conn.close()

# ================== END OF CORRECTED FUNCTION ==================

async def drop_all_tables():
    """Drops ALL application tables. EXTREMELY DANGEROUS."""
    tables_to_drop = [
        "security_events", "logs", "token_blacklist", "otps",
        "notifications", "sessions", "user_tokens", "param_stream",
        "video_stream", "workspace_members", "user_accounts", "workspaces", "users"
    ]

    db_manager = DatabaseManager()
    dropped_tables = []
    errors = {}

    try:
        async with db_manager.transaction() as conn: # conn is an asyncpg.Connection here
            for table_name in tables_to_drop:
                try:
                    # NOTE: asyncpg connections do not have 'escape_identifier'.
                    # For identifiers, f-string with double quotes is the standard way,
                    # but ensure table names are controlled and not from user input.
                    # Since table_name is from a hardcoded list, this is safe.
                    query = f'DROP TABLE IF EXISTS "{table_name}" CASCADE'
                    await conn.execute(query) # Use conn.execute directly
                    logger.info(f"Table '{table_name}' dropped successfully (async).")
                    dropped_tables.append(table_name)
                except asyncpg.PostgresError as db_err_inner: # Catch specific asyncpg errors
                    logger.error(f"PostgresError dropping table '{table_name}' (async): {db_err_inner}")
                    errors[table_name] = str(db_err_inner)
                    raise # Re-raise to stop the process
                except Exception as e_inner: # Catch other unexpected errors
                    logger.error(f"Generic error dropping table '{table_name}' (async): {e_inner}")
                    errors[table_name] = str(e_inner)
                    raise # Re-raise
        return {"message": "All tables dropped (async)", "dropped": dropped_tables, "errors": errors or "None"}
    except asyncpg.PostgresError as db_e: # Catch errors from transaction or higher level
        logger.error(f"Failed to drop tables due to PostgresError: {db_e}", exc_info=True)
        detail = {"message": f"PostgresError during table drop: {str(db_e)}", "collected_errors_before_failure": errors}
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=detail)
    except Exception as e:
        logger.error(f"Error during async drop_all_tables operation: {str(e)}", exc_info=True)
        detail_msg = {"message": "Internal error during async table drop operation.", "collected_errors_before_failure": errors}
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=detail_msg)


async def ensure_database_indices():
    """Ensure all necessary database indices exist (async)."""
    indices = [
        # ... (list of indices remains the same)
        "CREATE INDEX IF NOT EXISTS idx_workspaces_name ON workspaces(name)",
        "CREATE INDEX IF NOT EXISTS idx_users_username ON users(username) INCLUDE (user_id, role, is_active)",
        "CREATE INDEX IF NOT EXISTS idx_users_email ON users(email) INCLUDE (user_id)",
        "CREATE INDEX IF NOT EXISTS idx_otps_expires_at ON otps(expires_at)",
        "CREATE INDEX IF NOT EXISTS idx_otps_email_purpose ON otps(email, purpose)",
        "CREATE INDEX IF NOT EXISTS idx_security_events_user_id ON security_events(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_security_events_event_type ON security_events(event_type)",
        "CREATE INDEX IF NOT EXISTS idx_security_events_created_at ON security_events(created_at)",
        "CREATE INDEX IF NOT EXISTS idx_logs_user_id ON logs(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_logs_status ON logs(status)",
        "CREATE INDEX IF NOT EXISTS idx_logs_created_at ON logs(created_at)",
        "CREATE INDEX IF NOT EXISTS idx_token_blacklist_user_id_expires ON token_blacklist(user_id, expires_at)",
        "CREATE INDEX IF NOT EXISTS idx_notifications_user_id ON notifications(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_sessions_expires_at ON sessions(expires_at)",
        "CREATE INDEX IF NOT EXISTS idx_user_tokens_user_id ON user_tokens(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_user_tokens_is_active_refresh_exp ON user_tokens(is_active, refresh_expires_at)",
        "CREATE INDEX IF NOT EXISTS idx_param_stream_user_id ON param_stream(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_param_stream_workspace_id_unique ON param_stream(workspace_id)",
        "CREATE INDEX IF NOT EXISTS idx_workspace_members_workspace_id ON workspace_members(workspace_id)",
        "CREATE INDEX IF NOT EXISTS idx_workspace_members_user_id ON workspace_members(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_user_accounts_user_id ON user_accounts(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_video_stream_user_id ON video_stream(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_video_stream_is_streaming ON video_stream(is_streaming)",
        "CREATE INDEX IF NOT EXISTS idx_workspaces_is_active ON workspaces(is_active)",
        "CREATE INDEX IF NOT EXISTS idx_workspace_members_role ON workspace_members(role)",
        "CREATE INDEX IF NOT EXISTS idx_workspace_members_ws_user_role ON workspace_members(workspace_id, user_id, role)",
        "CREATE INDEX IF NOT EXISTS idx_users_role ON users(role)",
        "CREATE INDEX IF NOT EXISTS idx_users_last_login ON users(last_login DESC NULLS LAST) WHERE last_login IS NOT NULL",
        "CREATE INDEX IF NOT EXISTS idx_video_stream_workspace_id ON video_stream(workspace_id)",
        "CREATE INDEX IF NOT EXISTS idx_video_stream_status ON video_stream(status)",
        "CREATE INDEX IF NOT EXISTS idx_video_stream_ws_user ON video_stream(workspace_id, user_id)",
        "CREATE INDEX IF NOT EXISTS idx_video_stream_active_workspace ON video_stream(workspace_id, status) WHERE is_streaming = TRUE",
        "CREATE INDEX IF NOT EXISTS idx_sessions_workspace_id ON sessions(workspace_id)",
        "CREATE INDEX IF NOT EXISTS idx_sessions_active ON sessions(user_id, expires_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_user_tokens_workspace_id ON user_tokens(workspace_id)",
        "CREATE INDEX IF NOT EXISTS idx_user_tokens_refresh_token ON user_tokens(refresh_token) WHERE is_active = TRUE",
        "CREATE INDEX IF NOT EXISTS idx_user_tokens_access_token ON user_tokens(access_token) WHERE is_active = TRUE",
        "CREATE INDEX IF NOT EXISTS idx_user_tokens_active_user_refresh_exp ON user_tokens(user_id, refresh_expires_at DESC) WHERE is_active = TRUE",
        "CREATE INDEX IF NOT EXISTS idx_token_blacklist_token ON token_blacklist(token)",
        "CREATE INDEX IF NOT EXISTS idx_token_blacklist_user_id ON token_blacklist(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_logs_workspace_id ON logs(workspace_id)",
        "CREATE INDEX IF NOT EXISTS idx_logs_action_type_created_at ON logs(action_type, created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_logs_status_created_at ON logs(status, created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_security_events_workspace_id ON security_events(workspace_id)",
        "CREATE INDEX IF NOT EXISTS idx_security_events_event_type_created_at ON security_events(event_type, created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_security_events_severity_created_at ON security_events(severity, created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_notifications_workspace_id ON notifications(workspace_id)",
        "CREATE INDEX IF NOT EXISTS idx_notifications_stream_id ON notifications(stream_id)",
        "CREATE INDEX IF NOT EXISTS idx_notifications_unread ON notifications(user_id, is_read) WHERE is_read = FALSE",
        "CREATE INDEX IF NOT EXISTS idx_notifications_ws_user_read ON notifications(workspace_id, user_id, is_read)",
    ]
    # Remove duplicates by converting to set and back to list
    unique_indices = sorted(list(set(indices)))


    validate_db_config(config)
    DB_HOST = config['database'].get('host', 'localhost')
    DB_PORT = config['database'].get('port', '5432')
    DB_NAME = config['database'].get('dbname', 'appdb')
    DB_USER = config['database'].get('user', 'postgres')
    DB_PASSWORD = config['database'].get('password', 'postgres')
    conn: Optional[asyncpg.Connection] = None
    errors = []
    try:
        conn = await asyncpg.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
        logger.info("Starting database index verification/creation...")
        async with conn.transaction():
            for i, index_stmt in enumerate(unique_indices):
                try:
                    await conn.execute(index_stmt)
                    # Extract index name for logging, be robust if format varies
                    index_name_part = index_stmt.split("CREATE INDEX IF NOT EXISTS ")[1].split(" ON ")[0] if "CREATE INDEX IF NOT EXISTS " in index_stmt else "Unknown Index"
                    logger.debug(f"Index statement {i+1}/{len(unique_indices)} executed: {index_name_part}...")
                except asyncpg.PostgresError as e:
                    index_name_part_err = index_stmt.split("CREATE INDEX IF NOT EXISTS ")[1].split(" ON ")[0] if "CREATE INDEX IF NOT EXISTS " in index_stmt else "Unknown Index"
                    logger.error(f"Error executing index statement '{index_name_part_err}': {e}")
                    errors.append(f"Index '{index_name_part_err}': {str(e)}")
        logger.info("Database indices have been successfully verified/created (async).")
        if errors:
            logger.warning(f"Some non-critical errors occurred during index creation: {errors}")
    except asyncpg.PostgresError as e:
        logger.error(f"Error creating database indices (async): {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to create database indices: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error during index creation (async): {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Unexpected error during index creation")
    finally:
        if conn and not conn.is_closed():
            await conn.close()
            