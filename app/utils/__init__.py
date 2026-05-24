# utils/__init__.py
"""
Async utilities package for InsightEye system.
Provides modular utilities for various operations.
"""



# Parser utilities
from app.utils.parser_utils import (
    parse_string_or_list,
    parse_camera_ids,
    parse_date_format,
    parse_time_string
)


# Email utilities
from app.utils.email_utils import (
    send_email,
    send_email_from_client_to_admin,
    send_fire_alert_email,
    send_people_count_alert_email,
    send_shoplifting_alert_email,
)

# Image utilities
from app.utils.image_utils import (
    frame_to_base64,
    encoded_string
)

# Prediction utilities
from app.utils.prediction_utils import (
    make_prediction,
    use_fallback_prediction
)

# LLM utilities
from app.utils.llm_utils import (
    get_ChatOllama_model,
    generate_chat_response,
    format_chat_history
)

# uuid utilities
from app.utils.uuid_utils import (
    ensure_uuid, 
    ensure_uuid_str
)

# permission utilities
from app.utils.permission_utils import (
    check_workspace_access
)

# stream utilities
from app.utils.stream_utils import (
    safe_close_websocket,
    send_ping,
    handle_mark_read_message
)


__all__ = [
    # Parsers
    'parse_string_or_list',
    'parse_camera_ids',
    'parse_date_format',
    'parse_time_string',
    
    # Email
    'send_email',
    'send_email_from_client_to_admin',
    'send_fire_alert_email',
    'send_people_count_alert_email',
    'send_shoplifting_alert_email',
    
    # Image
    'frame_to_base64',
    'encoded_string',
    
    # Prediction
    'make_prediction',
    'use_fallback_prediction',
    
    # LLM
    'get_ChatOllama_model',
    'generate_chat_response',
    'format_chat_history',
    
    # uuid
    'ensure_uuid',
    'ensure_uuid_str',
    
    # permission
    'check_workspace_access',
    
    # stream
    'safe_close_websocket',
    'send_ping',
    'handle_mark_read_message'
    
]
