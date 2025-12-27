
# app/utils/stream_utils.py
from fastapi import WebSocket, WebSocketDisconnect
import logging
import asyncio
from uuid import UUID
from zoneinfo import ZoneInfo
from datetime import datetime, timezone
from typing import List, Optional
from starlette.websockets import WebSocketState
import logging
from app.config.settings import config
from app.services.database import db_manager

logger = logging.getLogger(__name__)


async def safe_close_websocket(websocket: WebSocket, username_for_log: Optional[str] = None):
    """Safely close a WebSocket connection with proper state checking."""
    try:
        current_state = websocket.client_state
        
        if current_state == WebSocketState.CONNECTED:
            logger.debug(f"Closing websocket for {username_for_log}, state: {current_state}")
            await websocket.close()
        elif current_state == WebSocketState.CONNECTING:
            logger.debug(f"Websocket connecting for {username_for_log}, waiting before close")
            await asyncio.sleep(0.1)
            if websocket.client_state == WebSocketState.CONNECTED:
                await websocket.close()
        elif current_state == WebSocketState.DISCONNECTED:
            logger.debug(f"Websocket already disconnected for {username_for_log}")
        elif current_state == WebSocketState.CLOSING:
            logger.debug(f"Websocket already closing for {username_for_log}")
            
    except RuntimeError as e:
        error_msg = str(e).lower()
        if any(phrase in error_msg for phrase in [
            "websocket is not connected",
            "already closed",
            "cannot call",
            "close message has been sent"
        ]):
            logger.debug(f"Websocket already closed for {username_for_log}: {e}")
        else:
            logger.warning(f"RuntimeError closing websocket for {username_for_log}: {e}")
    except Exception as e:
        logger.warning(f"Unexpected error closing websocket for {username_for_log}: {e}")


async def send_ping(websocket: WebSocket):
    """Send periodic pings to keep WebSocket connection alive."""
    try:
        ping_interval = float(config.get("websocket_ping_interval", 30.0))
        while websocket.client_state == WebSocketState.CONNECTED:
            await asyncio.sleep(ping_interval)
            if websocket.client_state == WebSocketState.CONNECTED:
                try:
                    await websocket.send_json({
                        "type": "ping",
                        "timestamp": datetime.now(ZoneInfo("Africa/Cairo")).timestamp()
                    })
                except RuntimeError as e:
                    if "close message has been sent" in str(e).lower():
                        logger.debug("Ping failed: WebSocket already closing")
                        break
                    else:
                        raise
            else:
                break
    except (WebSocketDisconnect, asyncio.CancelledError, ConnectionResetError, RuntimeError):
        logger.debug("Ping task for WebSocket ended")
    except Exception as e:
        logger.error(f"Error in WebSocket ping task: {e}", exc_info=True)


async def handle_mark_read_message(message: dict, user_id_str: str, username_for_log: str, websocket: WebSocket):
    """Handle mark_read messages from WebSocket."""
    try:
        notif_ids_raw = message.get("notification_ids", [])
        if isinstance(notif_ids_raw, list) and user_id_str:
            updated_count = 0
            valid_notif_ids_to_mark: List[UUID] = []
            
            for nid_str_raw in notif_ids_raw:
                try:
                    valid_notif_ids_to_mark.append(UUID(str(nid_str_raw)))
                except ValueError:
                    logger.warning(f"Invalid notification ID format for mark_read from {username_for_log}: {nid_str_raw}")
            
            if valid_notif_ids_to_mark:
                for nid_uuid in valid_notif_ids_to_mark:
                    try:
                        res = await db_manager.execute_query(
                            "UPDATE notifications SET is_read = TRUE, updated_at = $1 WHERE notification_id = $2 AND user_id = $3 AND is_read = FALSE",
                            (datetime.now(ZoneInfo("Africa/Cairo")), nid_uuid, UUID(user_id_str)),
                            return_rowcount=True
                        )
                        if res and res > 0:
                            updated_count += 1
                    except Exception as e_mark:
                        logger.error(f"Error marking notification {nid_uuid} as read for {user_id_str}: {e_mark}")
            
            # Send acknowledgment
            if websocket.client_state == WebSocketState.CONNECTED:
                await websocket.send_json({
                    "type": "ack_mark_read",
                    "ids": notif_ids_raw,
                    "updated_count": updated_count
                })
    except Exception as e:
        logger.error(f"Error handling mark_read for {username_for_log}: {e}")


async def send_ping_with_stability_check(websocket: WebSocket, username_for_log: str):
    try:
        ping_interval = float(config.get("websocket_ping_interval", 45.0))  # Longer interval
        consecutive_ping_failures = 0
        max_ping_failures = 3
        
        while (websocket.client_state == WebSocketState.CONNECTED and 
               consecutive_ping_failures < max_ping_failures):
            await asyncio.sleep(ping_interval)
            
            if websocket.client_state == WebSocketState.CONNECTED:
                try:
                    await websocket.send_json({
                        "type": "ping", 
                        "timestamp": datetime.now(ZoneInfo("Africa/Cairo")).timestamp()
                    })
                    consecutive_ping_failures = 0  # Reset on success
                except Exception as e:
                    consecutive_ping_failures += 1
                    logging.warning(f"Ping failed for {username_for_log} (#{consecutive_ping_failures}): {e}")
                    if consecutive_ping_failures >= max_ping_failures:
                        break
            else:
                break
                
    except Exception as e:
        logging.debug(f"Ping task ended for {username_for_log}: {e}")
