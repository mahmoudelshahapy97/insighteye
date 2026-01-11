# app/celery_app.py
from celery import Celery
from app.config.settings import config
import logging

logger = logging.getLogger(__name__)

# Create Celery app
celery_app = Celery(
    'insighteye',
    broker=f'redis://{config.redis_host}:{config.redis_port}/{config.redis_db}',
    backend=f'redis://{config.redis_host}:{config.redis_port}/{config.redis_db}',
    include=['app.tasks.stream_tasks']
)

# Celery configuration
celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Africa/Cairo',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=3600,  # 1 hour max
    task_soft_time_limit=3000,  # 50 minutes soft limit
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=100,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    result_expires=86400,  # 24 hours
    broker_connection_retry_on_startup=True,
)

logger.info("Celery app initialized")