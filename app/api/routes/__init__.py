from fastapi import APIRouter
from app.api.routes import auth_router, camera_router, otp_router, \
    postgres_router, session_router, stream_router, async_stream_router, \
    user_router, workspace_router, analytics_router, celery_health, shoplifting_router, \
    blocked_exit_router, no_entry_zone_router

# Create v1 router
router = APIRouter()

router.include_router(auth_router.router) 
router.include_router(user_router.router) 
router.include_router(otp_router.router) 
router.include_router(camera_router.router) 
router.include_router(postgres_router.router) 
router.include_router(session_router.router) 
router.include_router(stream_router.router) 
router.include_router(async_stream_router.router) 
router.include_router(workspace_router.router) 
router.include_router(analytics_router.router)
router.include_router(celery_health.router)
router.include_router(shoplifting_router.router)
router.include_router(blocked_exit_router.router)
router.include_router(no_entry_zone_router.router)