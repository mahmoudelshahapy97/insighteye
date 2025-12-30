from fastapi import APIRouter
from app.api.routes import auth_router, camera_router, otp_router, \
    qdrant_chat_router, qdrant_router, elasticsearch_router, postgres_router, \
    session_router, stream_router_3, \
    text_chat_router, user_router, workspace_router, analytics_router, \
    analytics_qdrant_router, analytics_extended_router, monitoring_router 

# Create v1 router
router = APIRouter()

router.include_router(auth_router.router) 
router.include_router(user_router.router) 
router.include_router(otp_router.router) 
router.include_router(camera_router.router) 
router.include_router(qdrant_router.router) 
router.include_router(elasticsearch_router.router) 
router.include_router(postgres_router.router) 
router.include_router(session_router.router) 
router.include_router(stream_router_3.router) 
router.include_router(workspace_router.router) 
router.include_router(qdrant_chat_router.router) 
router.include_router(text_chat_router.router) 
router.include_router(analytics_router.router)
router.include_router(analytics_qdrant_router.router)
router.include_router(analytics_extended_router.router)
router.include_router(monitoring_router.router)