# db_reset.py
import asyncio
import logging
import os
from async_database import drop_all_tables as async_drop_all_tables, initialize_database as async_initialize_database, init_db_pool, close_db_pool

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


async def main_reset():
    """
    Asynchronously drops all tables and re-initializes the database.
    """
    # Initialize the pool for the reset script operations
    await init_db_pool()
    
    try:
        logger.info("Starting database reset (async)...")
        
        # Use the async drop_all_tables from database.py
        drop_result = await async_drop_all_tables()
        logger.info(f"Drop all tables result: {drop_result.get('message', 'No message')}")
        
        # Use the async initialize_database from database.py
        init_success = await async_initialize_database()
        if init_success:
            logger.info("Database initialized successfully with schema (async).")
        else:
            logger.error("Failed to initialize database (async).")
            
    except Exception as e:
        logger.error(f"Database reset failed (async): {e}", exc_info=True)
    finally:
        # Close the pool after operations
        await close_db_pool()

if __name__ == "__main__":
    # Ensure the current directory is the script's directory for finding SQL file
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    
    try:
        asyncio.run(main_reset())
    except Exception as e:
        logger.critical(f"An error occurred during the db_reset script execution: {e}", exc_info=True)
        