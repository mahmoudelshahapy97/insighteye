# async_database.py - Enhanced with safety checks

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

# ================== ENHANCED SAFETY FUNCTIONS ==================

async def check_existing_tables() -> List[str]:
    """Check which application tables already exist in the database."""
    # Updated to match your actual schema table names
    expected_tables = [
        "users", "workspaces", "user_accounts", "workspace_members",
        "param_stream", "video_stream", "user_tokens", "sessions",
        "notifications", "otps", "token_blacklist", "logs", "security_events"
    ]
    
    validate_db_config(config)
    DB_HOST = config['database'].get('host', 'localhost')
    DB_PORT = config['database'].get('port', '5432')
    DB_NAME = config['database'].get('dbname', 'appdb')
    DB_USER = config['database'].get('user', 'postgres')
    DB_PASSWORD = config['database'].get('password', 'postgres')

    conn: Optional[asyncpg.Connection] = None
    existing_tables = []
    
    try:
        conn = await asyncpg.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
        
        # Query to check existing tables
        query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = ANY($1)
            ORDER BY table_name
        """
        
        rows = await conn.fetch(query, expected_tables)
        existing_tables = [row['table_name'] for row in rows]
        
        return existing_tables
        
    except Exception as e:
        logger.error(f"Error checking existing tables: {e}", exc_info=True)
        return []
    finally:
        if conn and not conn.is_closed():
            await conn.close()

async def validate_schema_structure() -> Dict[str, bool]:
    """
    Validate that existing tables have the correct structure.
    This helps identify if tables exist but have wrong schema.
    """
    validate_db_config(config)
    DB_HOST = config['database'].get('host', 'localhost')
    DB_PORT = config['database'].get('port', '5432')
    DB_NAME = config['database'].get('dbname', 'appdb')
    DB_USER = config['database'].get('user', 'postgres')
    DB_PASSWORD = config['database'].get('password', 'postgres')

    conn: Optional[asyncpg.Connection] = None
    validation_results = {}
    
    # Key columns that must exist in each table
    required_columns = {
        "users": ["user_id", "username", "email", "is_active", "role"],
        "workspaces": ["workspace_id", "name", "is_active"],
        "user_accounts": ["password_id", "user_id", "password_hash"],
        "workspace_members": ["membership_id", "workspace_id", "user_id", "role"],
        "video_stream": ["stream_id", "workspace_id", "user_id", "name", "path", "status"],
        "param_stream": ["param_id", "workspace_id", "user_id", "frame_delay", "frame_skip", "conf"],
        "sessions": ["session_id", "user_id", "expires_at"],
        "user_tokens": ["token_id", "user_id", "access_token", "refresh_token", "is_active"],
        "notifications": ["notification_id", "workspace_id", "user_id", "message"],
        "otps": ["otp_id", "email", "purpose", "otp_hash", "expires_at"],
        "token_blacklist": ["blacklist_id", "user_id", "token", "expires_at"],
        "logs": ["log_id", "action_type", "status", "content", "created_at"],
        "security_events": ["event_id", "event_type", "severity", "event_data", "created_at"]
    }
    
    try:
        conn = await asyncpg.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
        
        for table_name, required_cols in required_columns.items():
            try:
                # Check if table exists
                table_exists = await conn.fetchval(
                    "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)",
                    table_name
                )
                
                if not table_exists:
                    validation_results[table_name] = False
                    continue
                
                # Check if required columns exist
                existing_columns = await conn.fetch(
                    "SELECT column_name FROM information_schema.columns WHERE table_name = $1",
                    table_name
                )
                existing_col_names = [row['column_name'] for row in existing_columns]
                
                # Check if all required columns are present
                missing_cols = [col for col in required_cols if col not in existing_col_names]
                validation_results[table_name] = len(missing_cols) == 0
                
                if missing_cols:
                    logger.warning(f"Table '{table_name}' missing columns: {missing_cols}")
                
            except Exception as e:
                logger.error(f"Error validating table '{table_name}': {e}")
                validation_results[table_name] = False
        
        return validation_results
        
    except Exception as e:
        logger.error(f"Error during schema validation: {e}", exc_info=True)
        return {}
    finally:
        if conn and not conn.is_closed():
            await conn.close()

async def safe_initialize_database(force_recreate: bool = False):
    """
    Safely initialize the database with enhanced checks to prevent data loss.
    Now includes schema structure validation.
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    sql_file_path = os.path.join(current_dir, "insighteye-query_v1.2.sql")

    # First, check what already exists
    existing_tables = await check_existing_tables()
    table_counts = await check_table_data_counts()
    schema_validation = await validate_schema_structure()
    
    # Define expected core tables that must exist for the application to work
    core_tables = [
        "users", "workspaces", "user_accounts", "workspace_members",
        "param_stream", "video_stream", "user_tokens", "sessions",
        "notifications", "otps", "token_blacklist", "logs", "security_events"
    ]
    
    missing_core_tables = [table for table in core_tables if table not in existing_tables]
    invalid_tables = [table for table, is_valid in schema_validation.items() if not is_valid]
    
    # Log current database state
    if existing_tables:
        logger.info(f"Found existing tables: {existing_tables}")
        for table, count in table_counts.items():
            if count > 0:
                logger.info(f"Table '{table}' contains {count} records")
            elif count == 0:
                logger.info(f"Table '{table}' exists but is empty")
            else:
                logger.warning(f"Could not determine record count for table '{table}'")
        
        if missing_core_tables:
            logger.warning(f"Missing core tables: {missing_core_tables}")
        
        if invalid_tables:
            logger.warning(f"Tables with invalid structure: {invalid_tables}")
    else:
        logger.info("No existing application tables found - safe to initialize")
    
    # Safety check: prevent accidental data loss
    has_data = any(count > 0 for count in table_counts.values() if count > 0)
    
    # Determine if we need to initialize
    needs_initialization = len(missing_core_tables) > 0 or len(invalid_tables) > 0
    
    if not needs_initialization:
        logger.info("Database schema is complete and valid. No initialization needed.")
        return True
    
    if has_data and not force_recreate:
        logger.warning("Database has existing data but needs schema updates.")
        logger.warning("This operation uses CREATE TABLE IF NOT EXISTS and should be safe.")
        logger.warning(f"Missing tables: {missing_core_tables}")
        logger.warning(f"Invalid tables: {invalid_tables}")
        logger.info("Proceeding with SAFE schema initialization...")
    elif force_recreate:
        logger.warning("FORCE RECREATE enabled - this may affect existing data!")
        logger.warning(f"Tables with data: {[table for table, count in table_counts.items() if count > 0]}")
    
    # Proceed with initialization using the safe SQL file
    return await execute_safe_schema_initialization(sql_file_path)

async def execute_safe_schema_initialization(sql_file_path: str) -> bool:
    """
    Execute the SQL schema file safely using CREATE TABLE IF NOT EXISTS.
    This should not cause data loss since your SQL file uses IF NOT EXISTS.
    """
    validate_db_config(config)
    DB_HOST = config['database'].get('host', 'localhost')
    DB_PORT = config['database'].get('port', '5432')
    DB_NAME = config['database'].get('dbname', 'appdb')
    DB_USER = config['database'].get('user', 'postgres')
    DB_PASSWORD = config['database'].get('password', 'postgres')

    conn: Optional[asyncpg.Connection] = None
    try:
        # Read the SQL file
        with open(sql_file_path, 'r') as sql_file:
            sql_content = sql_file.read()

        # Your SQL file structure: pre-transaction + main transaction
        script_parts = sql_content.split('BEGIN;', 1)
        if len(script_parts) != 2:
            raise RuntimeError(f"The SQL script at {sql_file_path} does not have the expected 'BEGIN;' separator.")

        pre_transaction_sql = script_parts[0].strip()
        main_transaction_sql = ('BEGIN;' + script_parts[1]).strip()

        conn = await asyncpg.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
        await setup_asyncpg_connection_types(conn)

        # Execute pre-transaction statements (extensions, functions)
        if pre_transaction_sql:
            logger.info("Executing pre-transaction statements (extensions, functions)...")
            await conn.execute(pre_transaction_sql)
            logger.info("Pre-transaction statements executed successfully.")

        # Execute main schema transaction
        logger.info(f"Executing main schema transaction from: {sql_file_path}")
        logger.info("Note: Using CREATE TABLE IF NOT EXISTS - existing tables will not be affected")
        
        await conn.execute(main_transaction_sql)
        logger.info("Database schema successfully initialized/verified with asyncpg.")
        
        # Verify that all core tables now exist and are valid
        final_tables = await check_existing_tables()
        final_validation = await validate_schema_structure()
        
        core_tables = [
            "users", "workspaces", "user_accounts", "workspace_members",
            "param_stream", "video_stream", "user_tokens", "sessions",
            "notifications", "otps", "token_blacklist", "logs", "security_events"
        ]
        
        still_missing = [table for table in core_tables if table not in final_tables]
        still_invalid = [table for table, is_valid in final_validation.items() if not is_valid and table in final_tables]
        
        if still_missing:
            logger.error(f"Schema initialization completed but core tables still missing: {still_missing}")
            return False
        elif still_invalid:
            logger.error(f"Schema initialization completed but tables still invalid: {still_invalid}")
            return False
        else:
            logger.info("All core tables verified to exist and have correct structure after initialization.")
            return True
            
    except FileNotFoundError:
        logger.error(f"SQL schema file not found: {sql_file_path}")
        raise
    except Exception as e:
        logger.error(f"Error during safe schema initialization: {e}", exc_info=True)
        return False
    finally:
        if conn and not conn.is_closed():
            await conn.close()

# Add diagnostic endpoint to help debug schema issues
async def diagnose_database_state() -> Dict[str, Any]:
    """Comprehensive database state diagnosis for debugging."""
    try:
        existing_tables = await check_existing_tables()
        table_counts = await check_table_data_counts()
        schema_validation = await validate_schema_structure()
        
        # Get all tables in the database, not just expected ones
        validate_db_config(config)
        DB_HOST = config['database'].get('host', 'localhost')
        DB_PORT = config['database'].get('port', '5432')
        DB_NAME = config['database'].get('dbname', 'appdb')
        DB_USER = config['database'].get('user', 'postgres')
        DB_PASSWORD = config['database'].get('password', 'postgres')

        conn = await asyncpg.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
        
        try:
            all_tables_result = await conn.fetch(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name"
            )
            all_tables = [row['table_name'] for row in all_tables_result]
        finally:
            await conn.close()
        
        core_tables = [
            "users", "workspaces", "user_accounts", "workspace_members",
            "param_stream", "video_stream", "user_tokens", "sessions",
            "notifications", "otps", "token_blacklist", "logs", "security_events"
        ]
        
        missing_core_tables = [table for table in core_tables if table not in existing_tables]
        extra_tables = [table for table in all_tables if table not in core_tables]
        
        return {
            "all_tables_in_database": all_tables,
            "expected_core_tables": core_tables,
            "existing_core_tables": existing_tables,
            "missing_core_tables": missing_core_tables,
            "extra_tables": extra_tables,
            "table_record_counts": table_counts,
            "schema_validation": schema_validation,
            "total_records": sum(count for count in table_counts.values() if count > 0),
            "tables_with_data": [table for table, count in table_counts.items() if count > 0],
            "empty_tables": [table for table, count in table_counts.items() if count == 0],
            "schema_complete": len(missing_core_tables) == 0,
            "schema_valid": all(schema_validation.values())
        }
        
    except Exception as e:
        logger.error(f"Error during database diagnosis: {e}", exc_info=True)
        return {"error": str(e)}

async def check_table_data_counts() -> Dict[str, int]:
    """Check data counts in existing tables to prevent accidental data loss."""
    existing_tables = await check_existing_tables()
    table_counts = {}
    
    if not existing_tables:
        return table_counts
    
    validate_db_config(config)
    DB_HOST = config['database'].get('host', 'localhost')
    DB_PORT = config['database'].get('port', '5432')
    DB_NAME = config['database'].get('dbname', 'appdb')
    DB_USER = config['database'].get('user', 'postgres')
    DB_PASSWORD = config['database'].get('password', 'postgres')

    conn: Optional[asyncpg.Connection] = None
    
    try:
        conn = await asyncpg.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
        
        for table in existing_tables:
            try:
                # Use quoted identifier to handle any special characters
                count_query = f'SELECT COUNT(*) FROM "{table}"'
                count = await conn.fetchval(count_query)
                table_counts[table] = count
            except Exception as e:
                logger.warning(f"Could not get count for table '{table}': {e}")
                table_counts[table] = -1  # Indicate error
                
        return table_counts
        
    except Exception as e:
        logger.error(f"Error checking table data counts: {e}", exc_info=True)
        return table_counts
    finally:
        if conn and not conn.is_closed():
            await conn.close()

# Keep the original function for backward compatibility
async def initialize_database():
    """Initialize the database with safety checks (backward compatibility wrapper)."""
    return await safe_initialize_database(force_recreate=False)

async def drop_all_tables():
    """
    Drops ALL application tables. EXTREMELY DANGEROUS.
    Enhanced with additional safety checks.
    """
    # First check what exists and warn about data
    existing_tables = await check_existing_tables()
    table_counts = await check_table_data_counts()
    
    if not existing_tables:
        logger.info("No application tables found to drop.")
        return {"message": "No tables to drop", "dropped": [], "errors": "None"}
    
    # Warn about data loss
    total_records = sum(count for count in table_counts.values() if count > 0)
    if total_records > 0:
        logger.warning(f"WARNING: About to drop tables containing {total_records} total records!")
        for table, count in table_counts.items():
            if count > 0:
                logger.warning(f"  - {table}: {count} records")
    
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

# Add these functions to async_database.py for data recovery

async def check_for_backup_tables() -> Dict[str, bool]:
    """Check if backup tables exist that might contain recoverable data."""
    backup_patterns = ['_backup', '_bak', '_old', '_temp']
    
    validate_db_config(config)
    DB_HOST = config['database'].get('host', 'localhost')
    DB_PORT = config['database'].get('port', '5432')
    DB_NAME = config['database'].get('dbname', 'appdb')
    DB_USER = config['database'].get('user', 'postgres')
    DB_PASSWORD = config['database'].get('password', 'postgres')

    conn: Optional[asyncpg.Connection] = None
    backup_tables = {}
    
    try:
        conn = await asyncpg.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
        
        # Check for any tables that might be backups
        query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND (table_name LIKE '%_backup' 
                 OR table_name LIKE '%_bak' 
                 OR table_name LIKE '%_old' 
                 OR table_name LIKE '%_temp')
            ORDER BY table_name
        """
        
        rows = await conn.fetch(query)
        for row in rows:
            table_name = row['table_name']
            # Check if table has data
            try:
                count = await conn.fetchval(f'SELECT COUNT(*) FROM "{table_name}"')
                backup_tables[table_name] = count > 0
                if count > 0:
                    logger.info(f"Found backup table '{table_name}' with {count} records")
            except Exception as e:
                logger.warning(f"Could not check backup table '{table_name}': {e}")
                backup_tables[table_name] = False
        
        return backup_tables
        
    except Exception as e:
        logger.error(f"Error checking for backup tables: {e}", exc_info=True)
        return {}
    finally:
        if conn and not conn.is_closed():
            await conn.close()


async def restore_from_backup_table(backup_table: str, target_table: str) -> bool:
    """Restore data from a backup table to the main table."""
    validate_db_config(config)
    DB_HOST = config['database'].get('host', 'localhost')
    DB_PORT = config['database'].get('port', '5432')
    DB_NAME = config['database'].get('dbname', 'appdb')
    DB_USER = config['database'].get('user', 'postgres')
    DB_PASSWORD = config['database'].get('password', 'postgres')

    conn: Optional[asyncpg.Connection] = None
    
    try:
        conn = await asyncpg.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
        
        async with conn.transaction():
            # First, check if backup table exists and has data
            backup_count = await conn.fetchval(f'SELECT COUNT(*) FROM "{backup_table}"')
            if backup_count == 0:
                logger.warning(f"Backup table '{backup_table}' is empty")
                return False
            
            # Check if target table exists
            target_exists = await conn.fetchval(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)",
                target_table
            )
            
            if not target_exists:
                logger.error(f"Target table '{target_table}' does not exist")
                return False
            
            # Get column names from both tables to ensure compatibility
            backup_columns = await conn.fetch(
                "SELECT column_name FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position",
                backup_table
            )
            target_columns = await conn.fetch(
                "SELECT column_name FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position",
                target_table
            )
            
            backup_col_names = [row['column_name'] for row in backup_columns]
            target_col_names = [row['column_name'] for row in target_columns]
            
            # Find common columns
            common_columns = [col for col in backup_col_names if col in target_col_names]
            
            if not common_columns:
                logger.error(f"No common columns between '{backup_table}' and '{target_table}'")
                return False
            
            columns_str = ', '.join(f'"{col}"' for col in common_columns)
            
            # Clear target table first (optional - you might want to merge instead)
            await conn.execute(f'DELETE FROM "{target_table}"')
            
            # Insert data from backup
            insert_query = f'''
                INSERT INTO "{target_table}" ({columns_str})
                SELECT {columns_str} FROM "{backup_table}"
            '''
            
            result = await conn.execute(insert_query)
            restored_count = int(result.split()[-1]) if result.split()[-1].isdigit() else 0
            
            logger.info(f"Successfully restored {restored_count} records from '{backup_table}' to '{target_table}'")
            return True
            
    except Exception as e:
        logger.error(f"Error restoring from backup table '{backup_table}': {e}", exc_info=True)
        return False
    finally:
        if conn and not conn.is_closed():
            await conn.close()


# FIXED: Backup function without the validate_db_config call
async def backup_existing_data():
    """Create backup tables for all existing data (used internally for safety)."""
    existing_tables = await check_existing_tables()
    table_counts = await check_table_data_counts()
    
    # Only backup tables that have data
    tables_to_backup = [table for table, count in table_counts.items() if count > 0]
    
    if not tables_to_backup:
        return {"message": "No tables with data found to backup", "backed_up_tables": []}
    
    # REMOVED: validate_db_config(config) - Already validated in previous calls
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
