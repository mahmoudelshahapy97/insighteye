# main.py
from fastapi import FastAPI, Form, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from starlette.middleware.base import BaseHTTPMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from contextlib import asynccontextmanager
import secrets
import time
import logging
import signal
import asyncio
import uvicorn
from config import config
from user_manager import UserManager
from otp_manager import OTPManager
from session_manager import SessionManager
from camera import router as camera_router
from video_streaming_qdrant import router as video_router
from sessions_2 import router as session_router_2
from users import router as users_router
from otp import router as otp_router
from login_user import router as login_router
from text_chat import router as chat_router
from stream_one_video import router as stream_router_one
from stream_one_video import stream_manager
from qdrant_chat import router as qdrant_chat_router
from apscheduler.schedulers.background import BackgroundScheduler
from rate_limiter import RateLimiter
from rate_limit_middleware import RateLimitMiddleware
from token_expiration import TokenExpirationStrategy
from datetime import datetime
from database import initialize_database

# # Create tables if they don't exist
# logging.info("Create tables if they don't exist")
# initialize_database()
# logging.info("Created tables successfully")

class CacheControlMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        response = await call_next(request)
        
        # Set cache control headers based on path
        if request.url.path.startswith('/static/'):
            # Cache static files for 1 week
            response.headers['Cache-Control'] = 'public, max-age=604800'
        else:
            # Don't cache API responses
            response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, proxy-revalidate'
        
        return response

# Setup logging
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ])

# Get a logger instance (do this wherever you need to log)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Application starting up...")

    # # Initialize database
    # logger.info("Create tables if they don't exist")
    # initialize_database()
    # logger.info("Created tables successfully")

    # Start stream manager background tasks
    success = await stream_manager.start_background_tasks()
    if not success:
        logger.error("Failed to start stream manager background tasks")
    else:
        logger.info("Stream manager background tasks started successfully")
    
    # # Start scheduler
    # scheduler.start()

    yield
    # Shutdown
    logger.info("Application shutting down...")
    await stream_manager.shutdown()
    # scheduler.shutdown()
    # Any other cleanup tasks
    logger.info("Cleanup complete")

# Initialize FastAPI app
app = FastAPI(
    root_path="/insighteye",
    lifespan=lifespan,
    title="User Authentication Camera Management API",
    description="API for managing cameras and related system information",
    version="1.0.0",
    openapi_url=f"/openapi.json",
    docs_url=f"/docs",
    redoc_url=f"/redoc",
    )

# Initialize managers
user_manager = UserManager()
otp_manager = OTPManager()
session_manager = SessionManager()

origins = ["*"]#config.get("origins", ["https://insight-eye.vercel.app/", "http://16.170.216.227:8000", "http://16.170.216.227", "http://localhost:8000", "http://localhost:3000", "localhost"])
allowed_hosts = ["*"]#config.get("allowed_hosts", ["https://insight-eye.vercel.app/", "http://16.170.216.227:8000", "http://16.170.216.227", "http://localhost:8000", "http://localhost:3000", "http://localhost"])
session_timeout = config.get("session_timeout", 3600) # in seconds

# # Initialize rate limiter
# rate_limiter = RateLimiter(
#     redis_url=config.get("redis_url"),
#     default_limit=config.get("rate_limit_default", 60),
#     default_window=config.get("rate_limit_window", 60),
#     enable_in_memory_fallback=True
# )

# # Configure specific rate limits
# rate_limiter.configure_limit("/auth/login", limit=10, window=60)  # Stricter limits for login
# rate_limiter.configure_limit("/auth/register", limit=5, window=60)  # Stricter limits for registration
# rate_limiter.configure_limit("/api/*", limit=1000, window=60)  # General API limit

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # More secure than ["*"]
    allow_credentials=True,
    allow_methods=["*"],#["GET", "POST", "PUT", "DELETE"]
    allow_headers=["*"],#["Authorization", "Content-Type"]
    expose_headers=["X-Request-ID"]
)

# Add trusted hosts middleware
app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=allowed_hosts
    # allowed_hosts=["*"]  # In production, specify your actual domain/IP
)

# # Add rate limiting middleware
# app.add_middleware(
#     RateLimitMiddleware,
#     rate_limiter=rate_limiter,
#     user_extractor=SessionManager.get_current_user,
#     excluded_paths=["/health", "/metrics", "/static/"]  # Exclude these paths from rate limiting
# )

# Add compression middleware
app.add_middleware(GZipMiddleware, minimum_size=1000)

# Add cache control middleware
app.add_middleware(CacheControlMiddleware)

@app.middleware("http")
async def add_hsts_header(request: Request, call_next):
    response = await call_next(request)
    response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains; preload"
    return response

# Include routers
app.include_router(video_router)
# app.include_router(session_router)
app.include_router(session_router_2)
app.include_router(users_router)
app.include_router(login_router)
app.include_router(otp_router)#, prefix="/auth"
app.include_router(camera_router)
app.include_router(chat_router)
app.include_router(stream_router_one)
app.include_router(qdrant_chat_router)

def handle_sigint(signum, frame):
    """Stops all streams when Ctrl + C is pressed"""
    logging.info('SIGINT or CTRL-C detected')
    asyncio.run(shutdown_event())
    exit(0)

# # Initialize scheduler for background tasks
# scheduler = BackgroundScheduler()

def run_clean_expired_entries():
    loop = asyncio.get_event_loop()
    # loop.create_task(rate_limiter.clean_expired_entries())

# # Initialize database tables
# @app.on_event("startup")
# async def startup_event():
#     # Create tables if they don't exist
#     logging.info("Create tables if they don't exist")
#     initialize_database()
#     logging.info("Created tables successfully")

#     # Set up a scheduler to clean expired tokens automatically
#     scheduler.start()
    
#     # # Schedule token cleanup tasks
#     # scheduler.add_job(session_manager.clean_expired_tokens, 'interval', hours=1, 
#     #                  id='clean_expired_tokens', 
#     #                  replace_existing=True)
    
#     # scheduler.add_job(session_manager.clean_expired_blacklist, 'interval', hours=1, 
#     #                  id='clean_expired_blacklist', 
#     #                  replace_existing=True)

#     # scheduler.add_job(
#     #     lambda: asyncio.create_task(rate_limiter.clean_expired_entries()),
#     #     'interval', 
#     #     hours=1, 
#     #     id='clean_rate_limit_entries', 
#     #     replace_existing=True
#     # )
    
#     logger.info("Application startup complete")
    
# async def shutdown_event():
#     """Stops all streams when the app shuts down"""
#     from video_streaming import active_streams
#     for stream_id, stream_config in list(active_streams.items()):
#          stream_config['stop_event'].set()
#     logging.info("All streams stopped")

#     # Shutdown the scheduler
#     scheduler.shutdown()
#     logging.info("Background scheduler stopped")

@app.get("/health")
async def health_check():
    """API health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.now()}


if __name__ == "__main__":
    signal.signal(signal.SIGINT, handle_sigint)
    uvicorn.run("main:app", host=config.get("host", "127.0.0.1"), port=config.get("port", 8000))#, reload=True
    # uvicorn.run(
    #     "main:app", 
    #     host=config.get("host", "0.0.0.0"), 
    #     port=config.get("port", 8000),
    #     ssl_keyfile="/etc/ssl/private/nginx-selfsigned.key",  # Path to your key
    #     ssl_certfile="/etc/ssl/certs/nginx-selfsigned.crt"  # Path to your certificate
    # )


# # Middleware to add security headers to all responses
# @app.middleware("http")
# async def add_security_headers(request: Request, call_next):
#     response = await call_next(request)
    
#     # Security headers
#     response.headers["X-Content-Type-Options"] = "nosniff"
#     response.headers["X-Frame-Options"] = "DENY"
#     response.headers["Content-Security-Policy"] = "default-src 'self'"
#     response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains; preload"
#     response.headers["X-XSS-Protection"] = "1; mode=block"
#     response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    
#     # Remove server header - fixed method
#     if "server" in response.headers:
#         del response.headers["server"]
    
#     return response

# @app.middleware("http")
# async def add_security_headers(request: Request, call_next):
#     response = await call_next(request)
    
#     # Check if the connection is secure
#     is_secure = request.url.scheme == "https"
    
#     # Only add HSTS header if connection is secure
#     if is_secure:
#         response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains; preload"
        
#     # Add other security headers that are still useful even on HTTP
#     response.headers["X-Content-Type-Options"] = "nosniff"
#     response.headers["X-Frame-Options"] = "DENY"
#     response.headers["X-XSS-Protection"] = "1; mode=block"
#     response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    
#     # Modify CSP for HTTP
#     if is_secure:
#         response.headers["Content-Security-Policy"] = "default-src 'self'"
#     else:
#         # Less strict CSP for HTTP to allow loading resources
#         response.headers["Content-Security-Policy"] = "default-src 'self' http: https: 'unsafe-inline'"
    
#     # Remove server header if present
#     if "server" in response.headers:
#         del response.headers["server"]
    
#     return response



# # Add request tracking middleware
# class RequestTrackingMiddleware(BaseHTTPMiddleware):
#     async def dispatch(self, request: Request, call_next):
#         # Generate unique request ID
#         request_id = secrets.token_hex(16)
        
#         # Get client info for logging
#         client_host = request.client.host if request.client else "unknown"
        
#         # Log request
#         logger.info(f"Request {request_id} started: {request.method} {request.url.path} from {client_host}")
        
#         # Track timing
#         start_time = time.time()
        
#         # Process request
#         response = await call_next(request)
        
#         # Log response
#         process_time = time.time() - start_time
#         logger.info(f"Request {request_id} completed: {response.status_code} in {process_time:.4f}s")
        
#         # Add request ID to response
#         response.headers["X-Request-ID"] = request_id
        
#         return response

# app.add_middleware(RequestTrackingMiddleware)
