# database.py
import asyncpg
import psycopg2
import psycopg2.pool
import psycopg2.extras
from psycopg2 import pool
import logging
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Tuple, Union
from fastapi import HTTPException
from config import config
import os 


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__) # Use logger

# Database connection
def get_db_connection():
    """Establishes and returns a database connection."""
    try:
        conn = psycopg2.connect(
        dbname=config['database']['dbname'],
        user=config['database']['user'],
        password=config['database']['password'],
        host=config['database']['host'],
        port=config['database']['port']
    )
        return conn

    except psycopg2.OperationalError as e:
        raise HTTPException(status_code=500, detail=f"Database connection error: {e}")

def execute_db_query(query, params=None, fetch_one=False, fetch_all=False, return_rowcount=False):
    """Execute a database query with error handling and connection management."""
    conn = get_db_connection()
    cur = conn.cursor()
    result = None
    
    try:
        cur.execute(query, params or ())
        
        if fetch_one:
            result = cur.fetchone()
        elif fetch_all:
            result = cur.fetchall()
        elif return_rowcount:
            result = cur.rowcount
            
        conn.commit()
        return result
    except Exception as e:
        conn.rollback()
        logger.error(f"Database error: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        cur.close()
        conn.close()

def alter_table():
    """
    Drops ALL application tables. EXTREMELY DANGEROUS. Requires admin role.
    """

    try:
        query="""     
        ALTER TABLE video_stream
        ADD COLUMN last_activity TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        """ 

        execute_db_query(query)
    except Exception as e:
        logger.error(f"Error dropping table : {e}")

def print_table():
    """
    Drops ALL application tables. EXTREMELY DANGEROUS. Requires admin role.
    """

    try:
        query="""     
        SELECT * FROM video_stream;
        """ 

        print(execute_db_query(query, fetch_one=True)[0])
    except Exception as e:
        logger.error(f"Error dropping table : {e}")


def update_table():
    """
    Drops ALL application tables. EXTREMELY DANGEROUS. Requires admin role.
    """

    try:
        query= "UPDATE users SET is_subscribed = FALSE WHERE username = 'mahmoud'"

        execute_db_query(query)
    except Exception as e:
        logger.error(f"Error dropping table : {e}")

# update_table()

def drop_all_tables():
    """
    Drops ALL application tables. EXTREMELY DANGEROUS. Requires admin role.
    """
    try:

        tables_to_drop = [
            # Drop tables in reverse order of dependency or use CASCADE
            "security_events",
            "logs",
            "token_blacklist",
            "user_tokens",
            "sessions", # Might be redundant if only using user_tokens
            "param_stream_camera",
            "param_stream",
            "video_stream",
            "users" # Drop users last or use CASCADE on others
        ]
        
        dropped_tables = []
        errors = {}

        for table in tables_to_drop:
            try:
                # Use CASCADE cautiously, otherwise ensure correct drop order
                execute_db_query(f"DROP TABLE IF EXISTS {table} CASCADE")
                dropped_tables.append(table)
            except Exception as e:
                logger.error(f"Error dropping table '{table}': {e}")
                errors[table] = str(e)


    except Exception as e:
        logger.error(f"Error during drop_all_tables operation: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail="Internal server error during table drop operation."
        )

# Add this to database.py
def execute_db_transaction(queries_and_params):
    """Execute multiple queries in a single transaction"""
    conn = get_db_connection()
    cur = conn.cursor()
    results = []
    
    try:
        for query, params in queries_and_params:
            cur.execute(query, params or ())
            results.append(cur.fetchall() if cur.description else cur.rowcount)
        
        conn.commit()
        return results
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Transaction error: {e}")
    finally:
        cur.close()
        conn.close()

def ensure_database_indices():
    """Ensure all necessary database indices exist"""
    indices = [
        "CREATE INDEX IF NOT EXISTS idx_video_stream_user_id ON video_stream(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_video_stream_streaming ON video_stream(is_streaming)",
        "CREATE INDEX IF NOT EXISTS idx_video_stream_user_streaming ON video_stream(user_id, is_streaming)",
        "CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)",
        "CREATE INDEX IF NOT EXISTS idx_logs_user_id ON logs(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_logs_action_type ON logs(action_type)"
    ]
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        for index_stmt in indices:
            cur.execute(index_stmt)
        conn.commit()
        logging.info("Database indices have been verified")
    except Exception as e:
        conn.rollback()
        logging.error(f"Error creating database indices: {e}")
    finally:
        cur.close()
        conn.close()

def execute_complex_sql_file(file_path):
    """
    Reads and executes a complex SQL file that might contain transactions,
    function definitions, triggers, and other complex SQL constructs.
    
    Args:
        file_path: Path to the SQL file
        
    Returns:
        True if successful, raises an exception otherwise
    """
    try:
        # Read the SQL file
        with open(file_path, 'r') as sql_file:
            sql_content = sql_file.read()
        
        # Get a connection and execute the entire script at once
        # This preserves transaction integrity and complex SQL structures
        conn = get_db_connection()
        
        try:
            # Execute the entire script
            with conn.cursor() as cur:
                logging.info(f"Executing SQL file: {file_path}")
                cur.execute(sql_content)
            
            conn.commit()
            logging.info(f"Successfully executed SQL file: {file_path}")
            return True
            
        except Exception as e:
            conn.rollback()
            logging.error(f"Error executing SQL file: {e}")
            raise HTTPException(status_code=500, detail=f"SQL file execution error: {e}")
        finally:
            conn.close()
            
    except FileNotFoundError:
        logging.error(f"SQL file not found: {file_path}")
        raise HTTPException(status_code=404, detail=f"SQL file not found: {file_path}")
    except Exception as e:
        logging.error(f"Error reading SQL file: {e}")
        raise HTTPException(status_code=500, detail=f"Error reading SQL file: {e}")

def initialize_database():
    """Initialize the database with the InsightEye schema"""
    
    # Path to your SQL file - adjust as needed
    current_dir = os.path.dirname(os.path.abspath(__file__))
    sql_file_path = os.path.join(current_dir, "insighteye-query_v1.1.sql")
    
    try:
        # Execute the SQL file
        execute_complex_sql_file(sql_file_path)
        logging.info("Database schema successfully initialized")
        return True
    except Exception as e:
        logging.error(f"Failed to initialize database schema: {e}")
        return False

# Async connection pool and functions (using the imported asyncpg)
async def create_async_pool():
    """Create and return an asyncpg connection pool."""
    try:
        return await asyncpg.create_pool(
            user=config['database']['user'],
            password=config['database']['password'],
            database=config['database']['dbname'],
            host=config['database']['host'],
            port=config['database']['port'],
            min_size=1,
            max_size=10
        )
    except Exception as e:
        logging.error(f"Failed to create async connection pool: {e}")
        raise HTTPException(status_code=500, detail=f"Database connection error: {e}")

async def execute_async_query(pool, query, params=None, fetch_one=False, fetch_all=False):
    """Execute an async database query using asyncpg."""
    async with pool.acquire() as conn:
        try:
            if fetch_one:
                return await conn.fetchrow(query, *(params or ()))
            elif fetch_all:
                return await conn.fetch(query, *(params or ()))
            else:
                return await conn.execute(query, *(params or ()))
        except Exception as e:
            logging.error(f"Database query error: {e}")
            raise HTTPException(status_code=500, detail=f"Database error: {e}")

def execute_sql_file(file_path):
    """
    Reads and executes SQL commands from a file.
    
    Args:
        file_path: Path to the SQL file
        
    Returns:
        True if successful, raises an exception otherwise
    """
    try:
        # Read the SQL file
        with open(file_path, 'r') as sql_file:
            sql_content = sql_file.read()
        
        # Split the SQL file by semicolons to get individual queries
        # This simple approach works for basic SQL files without complex statements
        queries = [q.strip() for q in sql_content.split(';') if q.strip()]
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        try:
            # Execute each query
            for query in queries:
                if query:  # Skip empty queries
                    logging.info(f"Executing query: {query[:50]}...")
                    cur.execute(query)
            
            conn.commit()
            logging.info(f"Successfully executed all queries from {file_path}")
            return True
            
        except Exception as e:
            conn.rollback()
            logging.error(f"Error executing SQL file: {e}")
            raise HTTPException(status_code=500, detail=f"SQL file execution error: {e}")
        finally:
            cur.close()
            conn.close()
            
    except FileNotFoundError:
        logging.error(f"SQL file not found: {file_path}")
        raise HTTPException(status_code=404, detail=f"SQL file not found: {file_path}")
    except Exception as e:
        logging.error(f"Error reading SQL file: {e}")
        raise HTTPException(status_code=500, detail=f"Error reading SQL file: {e}")

def validate_video_source(source: str) -> str:
    """
    Validates if a video source is properly formatted.
    
    Args:
        source: Source URL or path to validate
        
    Returns:
        Validated source string
        
    Raises:
        ValueError: If source format is invalid
    """
    source = source.replace('\\', '/')
    
    # Check if source is a URL, 'local', or a valid file path
    if not source.startswith(('http://', 'https://', 'rtsp://')) and not source.lower() == "local":
        if not source.startswith("/"):
            if not (len(source) > 2 and source[1:3] == ":/"):
                raise ValueError("Source must start with 'http://' or 'https://' or 'rtsp://' or 'local' or local file path with / or with C:/")
    
    # Validate local video file extensions
    if source.lower() != "local" and not source.startswith(('http://', 'https://', 'rtsp://')):
        video_extensions = ['.mp4', '.avi', '.mov', '.mkv', '.wmv', '.flv', '.webm', '.m4v']
        if not any(source.lower().endswith(ext) for ext in video_extensions):
            raise ValueError("Local source must be a video file with extension: " + ", ".join(video_extensions))
    
    return source

# # Database connection pool
# connection_pool = None

# def initialize_db_pool(dbname, user, password, host, port, min_conn=1, max_conn=10):
#     """Initialize the database connection pool."""
#     global connection_pool
#     try:
#         connection_pool = psycopg2.pool.ThreadedConnectionPool(
#             min_conn, max_conn,
#             dbname=dbname,
#             user=user,
#             password=password,
#             host=host,
#             port=port
#         )
#         logger.info("Database connection pool initialized")
#     except psycopg2.Error as e:
#         logger.critical(f"Failed to initialize database pool: {e}")
#         raise

# @contextmanager
# def get_db_connection():
#     """Get a database connection from the pool with context management."""
#     conn = None
#     try:
#         conn = connection_pool.getconn()
#         # Register UUID adapter for consistent type handling
#         psycopg2.extras.register_uuid()
#         yield conn
#     except psycopg2.OperationalError as e:
#         logger.error(f"Database connection error: {e}")
#         raise HTTPException(status_code=500, detail="Database connection error")
#     finally:
#         if conn:
#             connection_pool.putconn(conn)

# @contextmanager
# def get_db_cursor(commit=False):
#     """Get a database cursor with automatic transaction management."""
#     with get_db_connection() as conn:
#         cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
#         try:
#             yield cur
#             if commit:
#                 conn.commit()
#         except psycopg2.Error as e:
#             conn.rollback()
#             logger.error(f"Database error: {e}")
#             error_message = str(e)
#             # Sanitize error messages for production
#             if "permission denied" in error_message.lower():
#                 raise HTTPException(status_code=403, detail="Permission denied")
#             elif "duplicate key" in error_message.lower():
#                 raise HTTPException(status_code=409, detail="Duplicate entry")
#             elif "not found" in error_message.lower():
#                 raise HTTPException(status_code=404, detail="Resource not found")
#             else:
#                 raise HTTPException(status_code=500, detail="Database error")
#         finally:
#             cur.close()

# def execute_query(query: str, params: Optional[tuple] = None, fetch_one: bool = False, 
#                  fetch_all: bool = False, return_rowcount: bool = False) -> Any:
#     """Execute a database query with consistent error handling."""
#     with get_db_cursor(commit=True) as cur:
#         cur.execute(query, params or ())
        
#         if fetch_one:
#             return cur.fetchone()
#         elif fetch_all:
#             return cur.fetchall()
#         elif return_rowcount:
#             return cur.rowcount
#         return None

# def execute_batch(queries_and_params: List[Tuple[str, tuple]]) -> bool:
#     """Execute multiple queries in a single transaction."""
#     with get_db_connection() as conn:
#         try:
#             with conn.cursor() as cur:
#                 for query, params in queries_and_params:
#                     cur.execute(query, params or ())
#             conn.commit()
#             return True
#         except psycopg2.Error as e:
#             conn.rollback()
#             logger.error(f"Batch execution error: {e}")
#             raise HTTPException(status_code=500, detail="Database batch operation failed")

# def format_uuid(value) -> str:
#     """Ensure consistent UUID formatting as string."""
#     if value is None:
#         return None
#     return str(value)

# def format_db_result(row, dict_cursor=True) -> Dict:
#     """Format database row results with proper type handling."""
#     if not row:
#         return None
    
#     result = dict(row) if dict_cursor else row
    
#     # Convert any UUID fields to strings for JSON serialization
#     if isinstance(result, dict):
#         for key, value in result.items():
#             if key.endswith('_id') and value is not None:
#                 result[key] = format_uuid(value)
    
#     return result

# # Create a connection pool
# connection_pool = psycopg2.pool.SimpleConnectionPool(
#     user=config['database']['user'],
#     password=config['database']['password'],
#     database=config['database']['dbname'],
#     host=config['database']['host'],
#     port=config['database']['port'],
#     minconn=1,
#     maxconn=10
# )

# def get_db_connection():
#     """Gets a connection from the connection pool."""
#     try:
#         return connection_pool.getconn()
#     except psycopg2.OperationalError as e:
#         raise HTTPException(status_code=500, detail=f"Database connection error: {e}")

# def release_db_connection(conn):
#     """Returns a connection to the connection pool."""
#     connection_pool.putconn(conn)

# Database connection pool
connection_pool = None

def init_db_pool(min_connections=1, max_connections=10):
    """Initialize the database connection pool."""
    global connection_pool
    
    try:
        # Get database connection parameters from environment variables
        DB_HOST = os.environ.get('DB_HOST', 'localhost')
        DB_PORT = os.environ.get('DB_PORT', '5432')
        DB_NAME = os.environ.get('DB_NAME', 'appdb')
        DB_USER = os.environ.get('DB_USER', 'postgres')
        DB_PASSWORD = os.environ.get('DB_PASSWORD', 'postgres')
        
        connection_pool = pool.ThreadedConnectionPool(
            min_connections,
            max_connections,
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        logger.info("Database connection pool initialized")
    except Exception as e:
        logger.error(f"Failed to initialize connection pool: {e}")
        raise

def get_connection():
    """Get a connection from the pool."""
    if connection_pool is None:
        init_db_pool()
    return connection_pool.getconn()

def release_connection(conn):
    """Return a connection to the pool."""
    if connection_pool is not None:
        connection_pool.putconn(conn)

class DatabaseManager:
    """Manages database connections and queries with connection pooling."""
    
    @staticmethod
    def execute_query(query, params=None, fetch_one=False, fetch_all=False, return_rowcount=False):
        """Execute a database query with connection pooling."""
        conn = get_connection()
        cur = conn.cursor()
        result = None
        
        try:
            cur.execute(query, params or ())
            
            if fetch_one:
                result = cur.fetchone()
            elif fetch_all:
                result = cur.fetchall()
            elif return_rowcount:
                result = cur.rowcount
                
            conn.commit()
            return result
        except Exception as e:
            conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            cur.close()
            release_connection(conn)


