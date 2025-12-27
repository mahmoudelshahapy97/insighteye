# Camera Streaming & Detection System

## Overview
Enterprise-grade video streaming and object detection system with workspace-based multi-tenancy, real-time alerts, and comprehensive camera management.

## Key Features

### 🎥 Camera Management
- **CRUD Operations**: Create, read, update, delete cameras with location metadata
- **Bulk Upload**: CSV import with location & alert settings
- **Location Hierarchy**: Building → Floor → Zone → Area → Location
- **Search & Filter**: Advanced filtering by location, status, type

### 🔄 Stream Processing
- **Shared Streams**: Multiple subscribers per video source (prevents file locks)
- **Object Detection**: YOLOv8 for people counting, gender detection, fire/smoke detection
- **Real-time Processing**: ~30 FPS with configurable frame skipping
- **Auto-restart**: Infinite retry logic for connection failures with exponential backoff

### 🚨 Alert System
- **People Count Alerts**: Threshold-based (greater/less than) with 10-min cooldown
- **Fire Detection**: Fire/smoke detection with 10-min cooldown, persistent across restarts
- **Notifications**: WebSocket + HTTP delivery, workspace-wide broadcasting

### 🏢 Workspace System
- **Multi-tenancy**: Users belong to workspaces with role-based access (owner/admin/member)
- **Quota Management**: Per-workspace camera limits based on member subscriptions
- **Isolated Data**: Cameras, detections, and settings scoped to workspaces

### 📊 Data Storage
- **PostgreSQL**: User/workspace/camera metadata, detection history, alerts
- **Qdrant**: Vector embeddings for frame storage and similarity search
- **Time-series**: Efficient querying by timestamp ranges

## Architecture

### Core Services

**StreamManager** (`stream_service.py`)
- Manages stream lifecycle (start/stop/restart)
- Enforces workspace quotas
- Handles retry logic and failure recovery
- Coordinates shared stream subscriptions

**CameraService** (`camera_service.py`)
- Camera CRUD operations
- Location hierarchy management
- Alert configuration
- Bulk operations

**StreamProcessingService** (`stream_processing_service.py`)
- Frame capture and processing
- YOLO model inference (people, gender, fire)
- Detection data persistence
- Alert triggering

**SharedStreamService** (`shared_stream_service.py`)
- Single-threaded video capture (prevents FFmpeg errors)
- Multi-subscriber support
- Automatic reconnection
- Frame rate control

### Database Schema

**Key Tables**:
- `users`: User accounts with camera limits
- `workspaces`: Tenant isolation
- `workspace_members`: User-workspace associations with roles
- `video_stream`: Camera metadata with location & alert settings
- `fire_detection_state`: Persistent fire alert state
- `people_count_alert_state`: People count alert state
- `notifications`: Real-time alerts

## API Endpoints

### Camera Management
```
POST   /camera/source                    # Create camera
GET    /camera/source/user               # List user's cameras
PUT    /camera/source                    # Batch update
DELETE /camera/source                    # Batch delete
GET    /camera/streams/locations/search  # Advanced search
```

### Stream Control
```
POST /streams3/start                     # Start stream
POST /streams3/stop                      # Stop stream (user_action)
POST /streams3/restart/{id}              # Restart stream
POST /streams3/workspace/{id}/start-all  # Start all workspace cameras
POST /streams3/workspace/{id}/stop-all   # Stop all workspace cameras
```

### Real-time
```
WS /streams3/ws/{id}        # Video frame streaming
WS /streams3/ws/notifications  # Alert notifications
```

### Location & Alerts
```
GET  /camera/locations/hierarchy         # Location tree
GET  /camera/locations/stats             # Location statistics
PUT  /camera/cameras/{id}/alert-settings # Update alerts
POST /camera/cameras/bulk-location-assignment  # Bulk assign
```

## Configuration

### Environment Variables
```bash
# Database
DATABASE_URL=postgresql://user:pass@host:5432/db

# Models
PEOPLE_MODEL_PATH=yolov8n.pt
GENDER_MODEL_PATH=gender.pt
FIRE_MODEL_PATH=fire.pt

# Processing
YOLO_MAX_INPUT_DIM=640
FRAME_SKIP=300  # Save every 300th frame
FRAME_DELAY=0.033  # 30 FPS
CONF_THRESHOLD=0.5

# Alerts
PEOPLE_COUNT_COOLDOWN_SECONDS=600
FIRE_COOLDOWN_SECONDS=600

# Streaming
MAX_STREAMS_PER_FILE=5
ENABLE_STREAM_SHARING=true
STREAM_HEALTHCHECK_INTERVAL_SECONDS=60
```

## Critical Behaviors

### Stop Reasons
- `user_action`: User-initiated stop → **NO auto-restart**, retry_count reset to 0
- `connection_error`/`timeout`/`system_error`: System failure → **Infinite retries** with backoff

### Database Fields
- `is_streaming`: TRUE = should be running, FALSE = should be stopped
- `stop_reason`: Indicates why stream stopped (user_action vs errors)
- `retry_count`/`next_retry_at`: Managed by retry_service

### Auto-restart Logic
1. Management loop checks `is_streaming=TRUE` in database
2. If not in memory → starts stream
3. If stream fails → records stop_reason, schedules retry (unless user_action)
4. Retry loop processes retries independently with exponential backoff

## Common Operations

### Start Camera
```python
await stream_manager.start_stream_in_workspace(
    stream_id=uuid,
    requester_user_id=user_id
)
```

### Stop Camera (User)
```python
await stream_manager.stop_stream_in_workspace(
    stream_id=uuid,
    requester_user_id=user_id,
    stop_reason='user_action',  # Critical!
    additional_context='Stopped by user'
)
```

### Bulk Upload
```bash
curl -X POST /camera/source/bulk-upload-with-location \
  -F "file=@cameras.csv" \
  -H "Authorization: Bearer $TOKEN"
```

## Monitoring

### Health Checks
- Stream processing stats (frames, detections, errors)
- Shared stream statistics
- Workspace quota usage
- Alert summaries

### Logs
- Stream lifecycle events (start/stop/restart)
- Detection data saves
- Alert triggers
- Error tracking with retry attempts

## Troubleshooting

**Camera won't auto-restart:**
- Check `stop_reason` (if `user_action`, it won't restart)
- Verify `is_streaming=TRUE` in database
- Check retry_service logs

**Shared stream errors:**
- Ensure only ONE stream reads from file
- Check FFmpeg threading is disabled
- Verify file permissions

**Alert not triggering:**
- Check cooldown hasn't expired (10 min default)
- Verify `alert_enabled=TRUE`
- Confirm thresholds are set

**Quota exceeded:**
- Check workspace limits: `GET /camera/workspaces/{id}/limits`
- Verify user subscriptions
- Review camera assignments

## Security Notes
- All endpoints require authentication via JWT tokens
- Workspace access validated via `check_workspace_access()`
- System admin role bypasses workspace restrictions
- User can only stop own cameras (unless workspace admin)

---

**Version**: 3.0  
**Last Updated**: 2025-12-24