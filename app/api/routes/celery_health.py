# app/routers/celery_health.py
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import JSONResponse
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from app.api.dependencies import require_system_admin

# System admins only, for every route on this router. These were previously
# unauthenticated, and /celery/tasks/{task_id}/revoke SIGKILLs the task, so
# anyone could kill stream-processing workers.
router = APIRouter(
    prefix="/system",
    tags=["System"],
    dependencies=[Depends(require_system_admin)],
)
logger = logging.getLogger(__name__)


@router.get("/celery/health")
async def celery_health():
    """
    Check Celery worker health.
    
    Returns status of:
    - Redis connection
    - Active workers
    - Queue status
    """
    try:
        from app.celery_app import celery_app
        from celery.result import AsyncResult
        import redis
        from app.config.settings import config
        
        health_status = {
            "timestamp": datetime.now(ZoneInfo("Africa/Cairo")).isoformat(),
            "celery": {},
            "redis": {},
            "workers": {}
        }
        
        # ==================== Redis Health ====================
        try:
            redis_client = redis.Redis(
                host=config.redis_host,
                port=config.redis_port,
                db=config.redis_db,
                password=config.redis_password,
                socket_timeout=5,
                socket_connect_timeout=5
            )
            
            # Ping Redis
            redis_client.ping()
            
            # Get Redis info
            redis_info = redis_client.info()
            
            health_status["redis"] = {
                "status": "healthy",
                "connected": True,
                "version": redis_info.get("redis_version"),
                "used_memory_human": redis_info.get("used_memory_human"),
                "connected_clients": redis_info.get("connected_clients"),
                "uptime_days": redis_info.get("uptime_in_days")
            }
            
        except Exception as redis_error:
            logger.error(f"Redis health check failed: {redis_error}")
            health_status["redis"] = {
                "status": "unhealthy",
                "connected": False,
                "error": str(redis_error)
            }
        
        # ==================== Celery Health ====================
        try:
            # Inspect active workers
            inspect = celery_app.control.inspect()
            
            # Get worker stats
            stats = inspect.stats()
            active_tasks = inspect.active()
            registered_tasks = inspect.registered()
            
            if stats:
                health_status["workers"] = {
                    "status": "healthy",
                    "active_workers": len(stats),
                    "worker_details": {}
                }
                
                # Worker details
                for worker_name, worker_stats in stats.items():
                    health_status["workers"]["worker_details"][worker_name] = {
                        "pool": worker_stats.get("pool", {}).get("implementation"),
                        "max_concurrency": worker_stats.get("pool", {}).get("max-concurrency"),
                        "active_tasks": len(active_tasks.get(worker_name, [])) if active_tasks else 0,
                        "registered_tasks": len(registered_tasks.get(worker_name, [])) if registered_tasks else 0
                    }
            else:
                health_status["workers"] = {
                    "status": "no_workers",
                    "active_workers": 0,
                    "warning": "No Celery workers are currently running"
                }
            
            # Queue stats
            reserved = inspect.reserved()
            scheduled = inspect.scheduled()
            
            health_status["celery"] = {
                "status": "healthy",
                "broker": str(celery_app.conf.broker_url).replace(config.redis_password or "", "***"),
                "backend": str(celery_app.conf.result_backend).replace(config.redis_password or "", "***"),
                "task_serializer": celery_app.conf.task_serializer,
                "result_serializer": celery_app.conf.result_serializer,
                "timezone": celery_app.conf.timezone,
                "queue_stats": {
                    "reserved_tasks": sum(len(v) for v in (reserved or {}).values()),
                    "scheduled_tasks": sum(len(v) for v in (scheduled or {}).values())
                }
            }
            
        except Exception as celery_error:
            logger.error(f"Celery health check failed: {celery_error}")
            health_status["celery"] = {
                "status": "unhealthy",
                "error": str(celery_error)
            }
        
        # ==================== Overall Status ====================
        redis_ok = health_status["redis"]["status"] == "healthy"
        workers_ok = health_status["workers"]["status"] == "healthy"
        celery_ok = health_status["celery"]["status"] == "healthy"
        
        overall_status = "healthy" if (redis_ok and celery_ok) else "degraded"
        
        if not workers_ok:
            overall_status = "warning"
        
        health_status["overall_status"] = overall_status
        
        # Determine HTTP status code
        http_status = status.HTTP_200_OK
        if overall_status == "degraded":
            http_status = status.HTTP_503_SERVICE_UNAVAILABLE
        elif overall_status == "warning":
            http_status = status.HTTP_200_OK  # Workers optional, API still works
        
        return JSONResponse(
            status_code=http_status,
            content=health_status
        )
        
    except Exception as e:
        logger.error(f"Health check error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Health check failed."
        )


@router.get("/celery/tasks/stats")
async def celery_task_stats():
    """
    Get Celery task statistics.
    
    Returns:
    - Task counts by state
    - Recent task history
    - Worker performance
    """
    try:
        from app.celery_app import celery_app
        import redis
        from app.config.settings import config
        
        redis_client = redis.Redis(
            host=config.redis_host,
            port=config.redis_port,
            db=config.redis_db,
            password=config.redis_password
        )
        
        # Get task keys from Redis
        task_keys = redis_client.keys("celery-task-meta-*")
        
        task_stats = {
            "timestamp": datetime.now(ZoneInfo("Africa/Cairo")).isoformat(),
            "total_tasks": len(task_keys),
            "tasks_by_state": {
                "PENDING": 0,
                "STARTED": 0,
                "PROGRESS": 0,
                "SUCCESS": 0,
                "FAILURE": 0,
                "RETRY": 0,
                "REVOKED": 0
            },
            "recent_tasks": []
        }
        
        # Count tasks by state
        for key in task_keys[:100]:  # Limit to 100 most recent
            try:
                task_data = redis_client.get(key)
                if task_data:
                    import json
                    task_info = json.loads(task_data)
                    state = task_info.get("status", "UNKNOWN")
                    
                    if state in task_stats["tasks_by_state"]:
                        task_stats["tasks_by_state"][state] += 1
                    
                    # Add to recent tasks
                    if len(task_stats["recent_tasks"]) < 10:
                        task_stats["recent_tasks"].append({
                            "task_id": task_info.get("task_id"),
                            "state": state,
                            "name": task_info.get("name"),
                            "result": task_info.get("result") if state == "SUCCESS" else None
                        })
            except:
                continue
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=task_stats
        )
        
    except Exception as e:
        logger.error(f"Task stats error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get task stats."
        )


@router.post("/celery/tasks/{task_id}/revoke")
async def revoke_task(task_id: str):
    """
    Revoke (cancel) a running task.
    
    Args:
        task_id: Celery task ID
    """
    try:
        from app.celery_app import celery_app
        from celery.result import AsyncResult
        
        task = AsyncResult(task_id, app=celery_app)
        
        # Revoke the task
        task.revoke(terminate=True, signal='SIGKILL')
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "success": True,
                "message": f"Task {task_id} has been revoked",
                "task_id": task_id
            }
        )
        
    except Exception as e:
        logger.error(f"Task revoke error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to revoke task."
        )
