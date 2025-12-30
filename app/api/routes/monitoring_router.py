# app/routers/monitoring_router.py
from fastapi import APIRouter, HTTPException
from datetime import datetime
from typing import Dict, Any
import asyncpg
from app.services.database import (
    get_pool_stats, 
    check_postgres_health, 
    connection_pool,
    db_manager
)
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/monitoring", tags=["Monitoring"])

@router.get("/health")
async def health_check():
    """
    Basic health check endpoint.
    Returns 200 if service is healthy, 503 otherwise.
    """
    try:
        is_healthy = await check_postgres_health()
        
        if not is_healthy:
            raise HTTPException(status_code=503, detail="Database unhealthy")
        
        return {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "database": "connected"
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail=f"Health check failed: {str(e)}")

@router.get("/database/pool")
async def get_database_pool_stats():
    """
    Get detailed connection pool statistics.
    Useful for monitoring and capacity planning.
    """
    try:
        stats = get_pool_stats()
        
        # Add additional metrics
        if connection_pool and not connection_pool._closed:
            stats['utilization_percent'] = (
                stats['active'] / stats['max_size'] * 100 
                if stats['max_size'] > 0 else 0
            )
            stats['available'] = stats['max_size'] - stats['active']
        
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "pool_stats": stats
        }
        
    except Exception as e:
        logger.error(f"Failed to get pool stats: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get pool stats: {str(e)}")

@router.get("/database/connections")
async def get_database_connections():
    """
    Get detailed information about active database connections.
    Shows what queries are running and connection states.
    """
    try:
        query = """
        SELECT 
            pid,
            usename,
            application_name,
            client_addr,
            state,
            state_change,
            query_start,
            EXTRACT(EPOCH FROM (NOW() - query_start)) as query_duration_seconds,
            wait_event_type,
            wait_event,
            LEFT(query, 100) as query_preview
        FROM pg_stat_activity
        WHERE datname = current_database()
        AND pid != pg_backend_pid()
        ORDER BY query_start DESC NULLS LAST
        LIMIT 50;
        """
        
        connections = await db_manager.execute_query(query, fetch_all=True)
        
        # Aggregate stats
        total = len(connections)
        active = sum(1 for c in connections if c.get('state') == 'active')
        idle = sum(1 for c in connections if c.get('state') == 'idle')
        idle_in_transaction = sum(1 for c in connections if c.get('state') == 'idle in transaction')
        waiting = sum(1 for c in connections if c.get('wait_event_type') is not None)
        
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "summary": {
                "total": total,
                "active": active,
                "idle": idle,
                "idle_in_transaction": idle_in_transaction,
                "waiting": waiting
            },
            "connections": connections
        }
        
    except Exception as e:
        logger.error(f"Failed to get connections: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get connections: {str(e)}")

@router.get("/database/performance")
async def get_database_performance():
    """
    Get database performance metrics including:
    - Cache hit ratio
    - Query statistics
    - Table statistics
    """
    try:
        # Cache hit ratio
        cache_query = """
        SELECT 
            sum(heap_blks_read) as heap_read,
            sum(heap_blks_hit) as heap_hit,
            ROUND(
                sum(heap_blks_hit) / NULLIF(sum(heap_blks_hit) + sum(heap_blks_read), 0) * 100,
                2
            ) as cache_hit_ratio
        FROM pg_statio_user_tables;
        """
        
        cache_stats = await db_manager.execute_query(cache_query, fetch_one=True)
        
        # Slow queries (requires pg_stat_statements extension)
        slow_query = """
        SELECT 
            LEFT(query, 100) as query_preview,
            calls,
            ROUND(total_exec_time::numeric, 2) as total_time_ms,
            ROUND(mean_exec_time::numeric, 2) as mean_time_ms,
            ROUND(max_exec_time::numeric, 2) as max_time_ms,
            ROUND((100 * total_exec_time / SUM(total_exec_time) OVER())::numeric, 2) as percent_total
        FROM pg_stat_statements
        WHERE query NOT LIKE '%pg_stat_statements%'
        ORDER BY mean_exec_time DESC
        LIMIT 10;
        """
        
        try:
            slow_queries = await db_manager.execute_query(slow_query, fetch_all=True)
        except:
            slow_queries = []
            logger.warning("pg_stat_statements not available")
        
        # Table statistics
        table_query = """
        SELECT 
            schemaname,
            relname as table_name,
            n_tup_ins as inserts,
            n_tup_upd as updates,
            n_tup_del as deletes,
            n_live_tup as live_rows,
            n_dead_tup as dead_rows,
            last_vacuum,
            last_autovacuum,
            last_analyze,
            last_autoanalyze
        FROM pg_stat_user_tables
        WHERE schemaname = 'public'
        ORDER BY n_tup_ins + n_tup_upd + n_tup_del DESC
        LIMIT 20;
        """
        
        table_stats = await db_manager.execute_query(table_query, fetch_all=True)
        
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "cache_hit_ratio": cache_stats.get('cache_hit_ratio') if cache_stats else None,
            "slow_queries": slow_queries,
            "table_statistics": table_stats
        }
        
    except Exception as e:
        logger.error(f"Failed to get performance metrics: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get performance: {str(e)}")

@router.get("/database/size")
async def get_database_size():
    """
    Get database and table size information.
    """
    try:
        # Database size
        db_size_query = """
        SELECT 
            pg_size_pretty(pg_database_size(current_database())) as database_size,
            pg_database_size(current_database()) as database_size_bytes;
        """
        
        db_size = await db_manager.execute_query(db_size_query, fetch_one=True)
        
        # Table sizes
        table_size_query = """
        SELECT 
            schemaname,
            tablename,
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size,
            pg_total_relation_size(schemaname||'.'||tablename) as total_size_bytes,
            pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as table_size,
            pg_size_pretty(pg_indexes_size(schemaname||'.'||tablename)) as indexes_size
        FROM pg_tables
        WHERE schemaname = 'public'
        ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
        LIMIT 20;
        """
        
        table_sizes = await db_manager.execute_query(table_size_query, fetch_all=True)
        
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "database": db_size,
            "tables": table_sizes
        }
        
    except Exception as e:
        logger.error(f"Failed to get size info: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get size: {str(e)}")

@router.get("/streams/stats")
async def get_stream_stats():
    """
    Get statistics about camera streams.
    """
    try:
        query = """
        SELECT 
            COUNT(*) as total_streams,
            COUNT(*) FILTER (WHERE is_streaming = true) as active_streams,
            COUNT(*) FILTER (WHERE is_streaming = false) as inactive_streams,
            COUNT(*) FILTER (WHERE status = 'active') as status_active,
            COUNT(*) FILTER (WHERE status = 'error') as status_error,
            COUNT(*) FILTER (WHERE status = 'inactive') as status_inactive,
            COUNT(*) FILTER (WHERE stop_reason = 'user_action') as user_stopped,
            COUNT(*) FILTER (WHERE stop_reason = 'error') as error_stopped
        FROM video_stream;
        """
        
        stats = await db_manager.execute_query(query, fetch_one=True)
        
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "stream_stats": stats
        }
        
    except Exception as e:
        logger.error(f"Failed to get stream stats: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get stream stats: {str(e)}")

@router.post("/database/pool/warmup")
async def warmup_pool():
    """
    Manually warm up the connection pool.
    Useful after restart or when anticipating high load.
    """
    try:
        if not connection_pool or connection_pool._closed:
            raise HTTPException(status_code=503, detail="Pool not initialized")
        
        min_size = connection_pool.get_min_size()
        
        # Pre-create connections
        connections = []
        for i in range(min_size):
            conn = await connection_pool.acquire(timeout=10)
            await conn.execute("SELECT 1")
            connections.append(conn)
        
        # Release them back
        for conn in connections:
            await connection_pool.release(conn)
        
        return {
            "status": "success",
            "warmed_connections": len(connections),
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Pool warmup failed: {e}")
        raise HTTPException(status_code=500, detail=f"Warmup failed: {str(e)}")
