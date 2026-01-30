# app/config/settings.py
from typing import List, Optional, Literal
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from dotenv import load_dotenv
import uuid
import platform
import socket
from uuid import UUID
from hashlib import sha256

load_dotenv()

def generate_deterministic_server_id() -> UUID:
    """
    Generate a deterministic server ID based on system information.
    This ensures the same ID is used across container restarts.
    """
    # Gather system information
    hostname = socket.gethostname()
    system = platform.system()
    node = platform.node()
    machine = platform.machine()
    
    # Create a unique string from system info
    unique_string = f"{hostname}-{system}-{node}-{machine}"
    
    # Generate a deterministic UUID from the hash
    hash_digest = sha256(unique_string.encode()).hexdigest()
    # Use first 32 characters of hash to create UUID
    return UUID(hash_digest[:32])

class Settings(BaseSettings):
    """
    Central application configuration.
    Loaded from:
    - Environment variables
    - .env
    - Defaults below
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Generate a deterministic ID for this server instance based on system info
    server_id: UUID = Field(default_factory=generate_deterministic_server_id)

    # capacity 
    max_local_streams: int = 100

    # Grace periods (adjust based on your RTSP cameras)
    zombie_detection_timeout_seconds: int = 600  # 10 minutes for RTSP
    rtsp_grace_period_seconds: int = 600  # 10 minutes
    file_grace_period_seconds: int = 120  # 2 minute

    # =============================================================================
    # PROJECT INFORMATION
    # =============================================================================
    project_name: str = "Insighteye API"
    project_description: str = "Insighteye API with various ML capabilities"
    project_version: str = "1.0.0"
    environment: Literal["development", "staging", "production"] = "production"

    # =============================================================================
    # APP
    # =============================================================================
    app_name: str = "Insighteye API"
    app_description: str = "Insighteye API with various ML capabilities"
    app_version: str = "1.0.0"
    app_api_prefix: str = "/api/v2"
    app_debug: bool = True
    app_reload: bool = True
    app_host: str = "127.0.0.1"
    app_port: int = 8000
    app_log_level: str = "info"
    app_workers: int = 8
    fastapi_root_path: str = "/insighteye"

    static_path_prefix: str = "/static/"
    api_path_prefix: str = "/api/"
    auth_path_prefix: str = "/auth/"
    static_cache_control: str = "public, max-age=604800"

    pythonunbuffered: int = 1
    pythondontwritebytecode: int = 1
    use_uvloop: bool = True

    # =============================================================================
    # CORS & HOSTS
    # =============================================================================
    cors_origins: List[str] = [
        "13.61.228.251",
        "http://13.61.228.251",
        "http://localhost:8000",
        "http://localhost:3000",
        "localhost",
    ]

    allow_origins: List[str] = [
        "13.61.228.251",
        "http://13.61.228.251",
        "http://localhost:8000",
        "http://localhost:3000",
        "localhost",
    ]

    trusted_hosts: List[str] = [
        "127.0.0.1",
        "localhost",
    ]

    cors_allow_credentials: bool = True
    cors_allow_methods: List[str] = ["GET", "POST", "PUT", "DELETE"]
    cors_allow_headers: List[str] = ["Content-Type", "Authorization", "X-Request-ID"]
    cors_expose_headers: str = "X-Request-ID"

    force_hsts: bool = False

    # =============================================================================
    # SECURITY / TOKENS
    # =============================================================================
    secret_key: str
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 1500
    refresh_token_expire_days: int = 15
    session_timeout: int = 600
    session_id: str = "2gXz1vQjW7-M6XsHwZp9D1BEXyL3oAqVfJbYK9t5U2c"

    otp_rate_limit_seconds: int = 60
    otp_expiration_seconds: int = 600
    otp_expiry: int = 300

    # =============================================================================
    # LOGGING
    # =============================================================================
    log_level: str = "INFO"
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    log_file_path: str = "/app/logs/app.log"
    log_sql_queries: bool = False

    # =============================================================================
    # DATABASE (POSTGRES + PGBOUNCER)
    # =============================================================================
    postgres_db: str = "insighteye_db"
    postgres_user: str = "insighteye_user"
    postgres_password: str
    db_host: str = "172.31.25.133"
    db_port: int = 6432

    db_min_pool_size: int = 50
    db_max_pool_size: int = 200
    db_timeout: float = 30.0
    db_command_timeout: float = 60.0

    db_max_queries: int = 50_000
    db_max_inactive_lifetime: float = 3600.0

    db_health_check_interval: int = 60
    db_max_retries: int = 10
    db_retry_delay_base: int = 2
    db_retry_delay_max: int = 30

    # =============================================================================
    # QDRANT
    # =============================================================================
    qdrant_host: str = "172.31.25.133"
    qdrant_url: str = "http://172.31.25.133"
    qdrant_port_http: int = 6333
    qdrant_port_grpc: int = 6334

    qdrant_collection_name: str = "person_counts"
    qdrant_query_timeout: int = 50
    qdrant_fetch_limit: int = 50
    qdrant_vector_size: int = 1

    # =============================================================================
    # DATA BACKEND
    # =============================================================================
    data_backend: Literal["qdrant", "elasticsearch"] = "qdrant"

    # =============================================================================
    # ELASTICSEARCH
    # =============================================================================
    elasticsearch_hosts: List[str] = ["http://172.31.25.133:9200"]
    elasticsearch_index_name: str = "person_counts"
    elasticsearch_timeout: float = 60.0
    elasticsearch_version: int = 8
    elasticsearch_shards: int = 1
    elasticsearch_replicas: int = 1
    elasticsearch_refresh_interval: str = "1s"

    prediction_data_points_limit: int = 1000

    # =============================================================================
    # EMAIL / SMTP
    # =============================================================================
    sender_email: str
    sender_password: str
    smtp_server: str = "smtp.gmail.com"
    smtp_port: int = 587
    smtp_username: str
    smtp_password: str
    otp_sender_email: str
    smtp_timeout: int = 30

    # =============================================================================
    # MODELS
    # =============================================================================
    yolo_config_dir: str = "/app/.ultralytics"
    # Model Backend Selection
    model_backend: Literal["pytorch", "onnx", "openvino", "tensorrt"] = "pytorch"

    # PyTorch Model Paths
    pt_people_model_path: str = "models/people.pt"
    pt_gender_model_path: str = "models/gender.pt"
    pt_fire_model_path: str = "models/fire.pt"

    # ONNX Model Paths
    onnx_people_model_path: str = "models/people.onnx"
    onnx_gender_model_path: str = "models/gender.onnx"
    onnx_fire_model_path: str = "models/fire.onnx" 

    # OpenVINO Model Paths
    openvino_people_model_path: str = "models/people_openvino"
    openvino_gender_model_path: str = "models/gender_openvino"
    openvino_fire_model_path: str = "models/fire_openvino"

    # TensorRT Model Paths
    tensorrt_people_model_path: str = "models/people.engine"
    tensorrt_gender_model_path: str = "models/gender.engine"
    tensorrt_fire_model_path: str = "models/fire.engine"

    # TensorRT Configuration
    tensorrt_precision: Literal["fp32", "fp16", "int8"] = "fp16"
    tensorrt_max_batch_size: int = 8
    tensorrt_workspace_size_mb: int = 2048  # 2GB workspace

    @property
    def people_model_path(self) -> str:
        if self.model_backend == "onnx":
            return self.onnx_people_model_path
        elif self.model_backend == "openvino":
            return self.openvino_people_model_path
        elif self.model_backend == "tensorrt":
            return self.tensorrt_people_model_path
        return self.pt_people_model_path

    @property
    def gender_model_path(self) -> str:
        if self.model_backend == "onnx":
            return self.onnx_gender_model_path
        elif self.model_backend == "openvino":
            return self.openvino_gender_model_path
        elif self.model_backend == "tensorrt":
            return self.tensorrt_gender_model_path
        return self.pt_gender_model_path

    @property
    def fire_model_path(self) -> str:
        if self.model_backend == "onnx":
            return self.onnx_fire_model_path
        elif self.model_backend == "openvino":
            return self.openvino_fire_model_path
        elif self.model_backend == "tensorrt":
            return self.tensorrt_fire_model_path
        return self.pt_fire_model_path

    model_cache_dir: str = "./model_cache"
    model_device: Literal["cpu", "cuda"] = "cuda"
    max_workers: int = 4

    jpeg_quality: int = 85

    # =============================================================================
    # CHAT / LLM
    # =============================================================================
    chat_embeddings: str = "mixedbread-ai/mxbai-embed-large-v1"
    chat_format: str = "json"

    chat_llama: str = "llama3.2:1b"
    chat_phi: str = "vanilj/Phi-4:Q8_0"
    chat_phi4: str = "phi4"
    chat_commandra: str = "command-r7b-arabic"
    chat_qwen: str = "qwen2.5:14b"
    chat_qwen_coder: str = "qwen2.5-coder:7b"

    chat_max_new_tokens: int = 1024
    chat_max_tokens: int = 1024
    chat_num_predict: int = 1024
    chat_temperature: float = 0.7
    chat_top_p: float = 0.95
    chat_top_k: int = 40
    chat_top_k_retriever: int = 2

    system_prompt: str = "You are a helpful assistant."
    text_chat_prompt: str = (
        "You are a helpful assistant that can answer questions about the text."
    )

    # =============================================================================
    # STREAM MANAGEMENT
    # =============================================================================
    stream_management_interval: float = 30.0
    stream_management_loop_interval: float = 30.0
    stream_batch_size: int = 50
    stream_batch_update_size: int = 50
    max_concurrent_starts: int = 10
    max_concurrent_stream_starts: int = 10
    max_concurrent_streams: int = 100
    max_streams_per_workspace: int = 100

    stream_db_update_interval: float = 10.0
    stream_db_activity_update_interval: float = 5.0
    stream_stale_threshold_seconds: float = 600.0
    stream_stop_timeout_seconds: float = 10.0
    stream_manager_poll_interval_seconds: float = 60.0  # 1 minute
    stream_manager_restart_delay_seconds: float = 15.0

    stream_cleanup_interval_seconds: int = 60
    stream_healthcheck_interval_seconds: int = 300  # 5 minutes (was 2-3 min)
    stream_start_stagger_delay: float = 2.0

    stream_ws_start_wait_attempts: int = 10
    websocket_client_fps: float = 15.0
    websocket_receive_timeout: float = 60.0
    websocket_ping_interval: float = 60.0

    # Per-camera resource limits
    max_read_retries_per_camera: int = 200  # Max retries before giving up
    read_retry_delay_base: float = 0.1      # Base delay between retries
    read_retry_delay_max: float = 2.0       # Max delay between retries
    camera_health_check_interval: int = 30  # Seconds between health checks

    # Multi-camera load balancing
    enable_camera_load_balancing: bool = True
    max_simultaneous_camera_starts: int = 3  # Start cameras in batches
    camera_start_delay: float = 2.0          # Delay between camera starts

    # =============================================================================
    # RTSP
    # =============================================================================
    rtsp_transport: Literal["tcp", "udp"] = "tcp"
    rtsp_timeout: int = 30
    rtsp_connection_timeout: int = 30
    rtsp_read_timeout: int = 180
    rtsp_reconnect_attempts: int = 10
    rtsp_buffer_size: int = 1
    max_reconnect_attempts: int = 5
    min_reconnect_interval: float = 3.0
    
    # Frame processing
    target_fps: float = 15.0
    max_frame_width: int = 640
    buffer_size: int = 1

    # ✅ OPTIMIZED: Aggressive error handling for RTSP stability
    max_consecutive_errors: int = 100      # Down from 1000 - force recovery faster
    max_read_timeout: float = 120.0        # Down from 180s - detect failures faster
    recovery_interval: float = 60.0        # Up from 5s - prevent recovery spam
    connection_health_timeout: float = 300.0  # Down from 600s - 5 min health check

    # Backoff settings
    min_backoff_delay: float = 1.0
    max_backoff_delay: float = 15.0
    backoff_multiplier: float = 1.5
    
    # Transport settings
    prefer_tcp: bool = True
    use_hw_accel: bool = False

    # =============================================================================
    # VIDEO / STREAM PERFORMANCE
    # =============================================================================
    cv_frame_buffer_size: int = 1
    frame_skip: int = 300
    yolo_input_size: int = 640
    people_confidence: float = 0.5
    gender_confidence: float = 0.5
    fire_confidence: float = 0.5
    people_device: Literal["cpu", "cuda"] = "cpu"

    enable_batch_inference: bool = True
    batch_size: int = 4
    batch_max_wait_ms: int = 100
    inference_workers: int = 2
    resource_monitor_interval: int = 300

    # =============================================================================
    # TORCH CPU OPTIMIZATION
    # =============================================================================
    torch_num_threads: int = 4
    torch_num_interop_threads: int = 2

    # =============================================================================
    # SHARED STREAMS
    # =============================================================================
    enable_stream_sharing: bool = True
    max_shared_streams: int = 50
    max_streams_per_file: int = 50
    shared_stream_buffer_size: int = 3
    shared_stream_timeout_seconds: int = 600
    shared_stream_max_fps: int = 30
    shared_stream_memory_limit_mb: int = 100

    log_shared_stream_stats: bool = True
    log_shared_stream_stats_interval: int = 30

    # =============================================================================
    # FILE / VIDEO MANAGEMENT
    # =============================================================================
    video_loop_enabled: bool = True
    video_restart_on_end: bool = True
    video_validation_enabled: bool = True
    validate_video_files_before_processing: bool = True
    video_file_validation_timeout: float = 10.0
    video_file_manager_cleanup_interval: int = 300
    max_video_restart_attempts: int = 10
    short_video_optimization: bool = True

    # =============================================================================
    # ALERTS & DETECTION
    # =============================================================================
    people_detect_interval: float = 2.0
    gender_detect_interval: float = 10.0
    fire_detect_interval: float = 5.0
    fire_state_cleanup_interval_seconds: int = 3600

    people_count_cooldown_duration_seconds: int = 300
    fire_cooldown_seconds: int = 600
    enable_people_count_email_alerts: bool = True

    # =============================================================================
    # MONITORING
    # =============================================================================
    enable_metrics: bool = True
    health_check_path: str = "/health"

    # =============================================================================
    # REDIS & CELERY
    # =============================================================================
    redis_host: str = "redis"  # Docker service name
    redis_port: int = 6379
    redis_db: int = 0
    redis_password: Optional[str] = None
    redis_max_connections: int = 50
    redis_socket_timeout: int = 5
    redis_socket_connect_timeout: int = 5
    
    # Celery configuration
    celery_broker_url: Optional[str] = None  # Will default to Redis
    celery_result_backend: Optional[str] = None  # Will default to Redis
    celery_task_serializer: str = "json"
    celery_result_serializer: str = "json"
    celery_accept_content: List[str] = ["json"]
    celery_timezone: str = "Africa/Cairo"
    celery_enable_utc: bool = True
    celery_worker_concurrency: int = 4
    celery_worker_prefetch_multiplier: int = 1
    celery_task_acks_late: bool = True
    celery_task_reject_on_worker_lost: bool = True
    celery_result_expires: int = 86400  # 24 hours
    celery_task_time_limit: int = 3600  # Max task duration (seconds)
    
    def get_celery_broker_url(self) -> str:
        """Get Celery broker URL."""
        if self.celery_broker_url:
            return self.celery_broker_url
        
        auth = f":{self.redis_password}@" if self.redis_password else ""
        return f"redis://{auth}{self.redis_host}:{self.redis_port}/{self.redis_db}"
    
    def get_celery_result_backend(self) -> str:
        """Get Celery result backend URL."""
        if self.celery_result_backend:
            return self.celery_result_backend
        
        auth = f":{self.redis_password}@" if self.redis_password else ""
        return f"redis://{auth}{self.redis_host}:{self.redis_port}/{self.redis_db}"

# Singleton
config = Settings()
