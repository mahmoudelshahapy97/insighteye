# utils/__init__.py
"""
Async utilities package for InsightEye system.
Provides modular utilities for various operations.
"""

# Qdrant utilities
from app.utils.qdrant_utils import (
    get_workspace_qdrant_collection_name,
    ensure_workspace_qdrant_collection_exists,
    BASE_QDRANT_COLLECTION_NAME
)

# Elasticsearch utilities
from app.utils.elasticsearch_utils import (
    get_workspace_elasticsearch_index_name,
    ensure_workspace_elasticsearch_index_exists,
    BASE_ELASTICSEARCH_INDEX_NAME
)

# Parser utilities
from app.utils.parser_utils import (
    parse_string_or_list,
    parse_camera_ids,
    parse_date_format,
    parse_iso_date,
    parse_time_string,
    validate_prompt,
    validate_text
)

# Datetime utilities
from app.utils.datetime_utils import (
    datetime_to_iso,
    iso_to_datetime,
    timestamp_to_iso,
    iso_to_timestamp
)

# Pagination utilities
from app.utils.pagination_utils import (
    paginate_list_get_page,
    paginate_list_all_pages
)

# Email utilities
from app.utils.email_utils import (
    send_email,
    send_email_from_client_to_admin,
    send_fire_alert_email,
    send_people_count_alert_email
)

# Image utilities
from app.utils.image_utils import (
    frame_to_base64,
    base64_to_frame,
    encoded_string
)

# Prediction utilities
from app.utils.prediction_utils import (
    make_prediction,
    use_fallback_prediction,
    make_prediction_all_cameras
)

# LLM utilities
from app.utils.llm_utils import (
    get_ChatOpenAI_model,
    get_ChatOllama_model,
    generate_chat_response,
    format_chat_history,
    get_user_message,
    get_assistant_message
)

# Exception utilities
from app.utils.exception_utils import (
    handle_exceptions
)

# uuid utilities
from app.utils.uuid_utils import (
    ensure_uuid, 
    ensure_uuid_str, 
    validate_uuid_list
)

# permission utilities
from app.utils.permission_utils import (
    check_workspace_access,
    check_resource_ownership
)

# logging utilities
from app.utils.logging_utils import (
    get_service_logger,
    log_user_action,
    log_error
)

# error utilities
from app.utils.error_utils import (
    handle_db_errors
)

# stream utilities
from app.utils.stream_utils import (
    safe_close_websocket,
    send_ping,
    handle_mark_read_message,
    send_ping_with_stability_check
)

__all__ = [
    # Qdrant
    'get_workspace_qdrant_collection_name',
    'ensure_workspace_qdrant_collection_exists',
    'BASE_QDRANT_COLLECTION_NAME',

    # Elasticsearch
    'get_workspace_elasticsearch_index_name',
    'ensure_workspace_elasticsearch_index_exists',
    'BASE_ELASTICSEARCH_INDEX_NAME',

    # Parsers
    'parse_string_or_list',
    'parse_camera_ids',
    'parse_date_format',
    'parse_iso_date',
    'parse_time_string',
    'validate_prompt',
    'validate_text',
    
    # Datetime
    'datetime_to_iso',
    'iso_to_datetime',
    'timestamp_to_iso',
    'iso_to_timestamp',
    
    # Pagination
    'paginate_list_get_page',
    'paginate_list_all_pages',
    
    # Email
    'send_email',
    'send_email_from_client_to_admin',
    'send_fire_alert_email',
    'send_people_count_alert_email',
    
    # Image
    'frame_to_base64',
    'base64_to_frame',
    'encoded_string',
    
    # Prediction
    'make_prediction',
    'use_fallback_prediction',
    'make_prediction_all_cameras',
    
    # LLM
    'get_ChatOpenAI_model',
    'get_ChatOllama_model',
    'generate_chat_response',
    'format_chat_history',
    'get_user_message',
    'get_assistant_message',
    
    # Exception
    'handle_exceptions',
    
    # uuid
    'ensure_uuid',
    'ensure_uuid_str',
    'validate_uuid_list',
    
    # permission
    'check_workspace_access',
    'check_resource_ownership',
    
    # logging
    'get_service_logger',
    'log_user_action',
    'log_error',
    
    # error
    'handle_db_errors',

    # stream
    'safe_close_websocket',
    'send_ping',
    'handle_mark_read_message',
    'send_ping_with_stability_check',
    
]
