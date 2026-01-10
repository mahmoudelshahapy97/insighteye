# 🔧 Critical Fixes: Server Lock & Auto-Restart Issues

## **Problem Analysis**

### **Issue 1: Camera Not Locked by Server**
The database shows `locked_by_server IS NULL` even though the camera is running, meaning:
1. ❌ No server owns the camera
2. ❌ No heartbeat updates
3. ❌ Distributed coordination broken

### **Issue 2: Camera Doesn't Restart After Server Restart**
After restarting the application, cameras that were running don't auto-start because:
1. ❌ `locked_by_server` was never set
2. ❌ Distributed manager doesn't recognize orphaned cameras
3. ❌ `is_streaming=TRUE` but no server claims it

---

## 🔍 **Root Cause**

Looking at the logs, the camera was started via the **API endpoint** (`/insighteye/start`), which bypasses the distributed manager's claiming system:

```python
# stream_service.py - start_stream_in_workspace()
await self.start_stream_background(...)  # ❌ Doesn't set locked_by_server
```

The distributed manager only sets `locked_by_server` when it **claims** cameras, not when the API starts them directly.

---

## ✅ **Solution 1: Fix API-Started Cameras**

### **Update `start_stream_background` in `stream_service.py`**

Add database lock **before** starting the processing task:

<artifact identifier="fix-stream-lock" type="application/vnd.ant.code" language="python" title="Fixed start_stream_background with server lock">
async def start_stream_background(
    self,
    stream_id: UUID,
    owner_id: UUID,
    owner_username: str,
    camera_name: str,
    source: str,
    workspace_id: UUID,
    location_info: Optional[Dict[str, Any]] = None
):
    """
    🎬 Start stream with health verification.
    
    ENHANCED: Now sets locked_by_server for API-initiated starts
    """
    stream_id_str = str(stream_id)
    workspace_id_str = str(workspace_id)

    logger.info(f"🎬 START: stream={stream_id_str}, camera={camera_name}")
    
    # Validate workspace limits
    try:
        can_start, reason = await self.can_start_stream_in_workspace(workspace_id, owner_id)
        if not can_start:
            logger.warning(f"Cannot start {stream_id_str}: {reason}")
            raise WorkspaceQuotaExceeded(reason)
    except Exception as e:
        logger.error(f"Validation error: {e}", exc_info=True)
        raise
    
    async with self._safe_stream_operation(stream_id_str, "start"):
        async with self._lock:
            # Check if already healthy
            if stream_id_str in self.active_streams:
                stream_info = self.active_streams[stream_id_str]
                task = stream_info.get('task')
                latest_frame = stream_info.get('latest_frame')
                last_frame_time = stream_info.get('last_frame_time')
                
                is_healthy = False
                if last_frame_time:
                    age = (datetime.now(ZoneInfo("Africa/Cairo")) - last_frame_time).total_seconds()
                    is_healthy = age < 30 and latest_frame is not None
                
                task_alive = task and not task.done()
                current_state = self.stream_states.get(stream_id_str)
                
                if current_state == StreamState.ACTIVE and is_healthy and task_alive:
                    logger.info(f"✅ {stream_id_str} already healthy")
                    return
                else:
                    logger.warning(f"🔧 {stream_id_str} unhealthy, restarting")
                    await self._stop_stream(stream_id_str, for_restart=True)
                    await asyncio.sleep(1.0)
            
            # Transition to STARTING
            await self._transition_stream_state(stream_id_str, StreamState.STARTING)
            
            # Register in workspace
            self.workspace_streams[workspace_id_str].add(stream_id_str)
            self.stream_workspaces[stream_id_str] = workspace_id_str
            
            # Initialize stream entry
            self.active_streams[stream_id_str] = {
                'status': 'starting',
                'task': None,
                'start_time': datetime.now(ZoneInfo("Africa/Cairo")),
                'location_info': location_info or {},
                'workspace_id': workspace_id,
                'source': source,
                'camera_name': camera_name,
                'username': owner_username,
                'user_id': owner_id,
                'clients': set(),
                'latest_frame': None,
                'last_frame_time': None,
                'last_heartbeat': datetime.now(ZoneInfo("Africa/Cairo"))
            }
            
            # Initialize fire detection
            self.fire_detection_states[stream_id_str] = {
                'status': 'no detection',
                'last_detection_time': None,
                'last_notification_time': None
            }
            self.fire_detection_frame_counts[stream_id_str] = 0

        try:
            # ✅ FIX: Claim camera lock in database (for API-initiated starts)
            from app.config.settings import config
            server_id = config.server_id
            
            claim_query = """
                UPDATE video_stream
                SET 
                    locked_by_server = $1,
                    server_heartbeat = NOW(),
                    is_streaming = TRUE,
                    status = 'processing',
                    stop_reason = NULL,
                    stopped_at = NULL,
                    stopped_by = NULL,
                    retry_count = 0,
                    last_retry_at = NULL,
                    next_retry_at = NULL,
                    auto_retry_enabled = TRUE,
                    updated_at = NOW(),
                    last_activity = NOW()
                WHERE stream_id = $2
                  AND (locked_by_server IS NULL OR locked_by_server = $1)
                RETURNING locked_by_server
            """
            
            result = await self.db_manager.execute_query(
                claim_query, 
                (server_id, stream_id),
                fetch_one=True
            )
            
            if not result:
                logger.error(f"❌ Failed to claim lock for {stream_id_str}")
                raise RuntimeError("Another server owns this camera")
            
            logger.info(f"🔒 Claimed lock for {stream_id_str} (server: {server_id})")
            
            # Initialize stats
            self.stream_processing_stats[stream_id_str] = {
                "frames_processed": 0,
                "detection_count": 0,
                "avg_processing_time": 0.0,
                "last_updated": datetime.now(ZoneInfo("Africa/Cairo")),
                "errors": 0
            }

            # Ensure Qdrant collection
            if self.qdrant_service:
                await self.qdrant_service.ensure_workspace_collection(workspace_id)

            # Create processing task
            stop_event = asyncio.Event()
            
            task = asyncio.create_task(
                self.processing_service.process_stream_with_sharing(
                    stream_id=stream_id,
                    camera_name=camera_name,
                    source=source,
                    owner_username=owner_username,
                    owner_id=owner_id,
                    workspace_id=workspace_id,
                    stop_event=stop_event,
                    location_info=location_info
                )
            )
            task.set_name(f"process_stream_{stream_id_str}")
            task.add_done_callback(
                lambda t: asyncio.create_task(
                    self._handle_stream_task_completion(stream_id_str, t)
                )
            )

            # Update stream info
            async with self._lock:
                self.active_streams[stream_id_str].update({
                    'stop_event': stop_event,
                    'task': task,
                    'status': 'active_pending'
                })
            
            # Register with shared stream manager
            self._shared_stream_registry[source].add(stream_id_str)
            
            # Transition to ACTIVE
            await self._transition_stream_state(stream_id_str, StreamState.ACTIVE)

            # Notify workspace
            await self._notify_workspace_stream_started(workspace_id, camera_name, owner_username)
            
            # Update metrics
            self.metrics['total_streams_started'] += 1
            
            logger.info(f"✅ {stream_id_str} started successfully")

        except Exception as e:
            logger.error(f"Failed to start {stream_id_str}: {e}", exc_info=True)
            
            # Cleanup on failure
            async with self._lock:
                self.active_streams.pop(stream_id_str, None)
                self.workspace_streams[workspace_id_str].discard(stream_id_str)
                self.stream_workspaces.pop(stream_id_str, None)
            
            self.stream_processing_stats.pop(stream_id_str, None)
            self._shared_stream_registry[source].discard(stream_id_str)
            
            await self._transition_stream_state(stream_id_str, StreamState.ERROR, force=True)
            
            # Release lock on failure
            release_query = """
                UPDATE video_stream
                SET locked_by_server = NULL, server_heartbeat = NULL
                WHERE stream_id = $1 AND locked_by_server = $2
            """
            await self.db_manager.execute_query(release_query, (stream_id, server_id))
            
            raise
</artifact>

---

## ✅ **Solution 2: Ensure Heartbeat Updates**

The `update_heartbeat_for_owned_cameras` is already in place but verify it's running:

<artifact identifier="verify-heartbeat" type="application/vnd.ant.code" language="python" title="Enhanced heartbeat with logging">
async def update_heartbeat_for_owned_cameras(self) -> int:
    """
    💓 Update heartbeat for cameras owned by this server.
    
    ENHANCED: Added more logging for debugging
    """
    async with self._lock:
        owned_stream_ids = list(self.active_streams.keys())
    
    if not owned_stream_ids:
        return 0
    
    heartbeat_query = """
        UPDATE video_stream 
        SET 
            server_heartbeat = NOW(),
            last_activity = CASE
                WHEN status = 'active' THEN NOW()
                ELSE last_activity
            END
        WHERE stream_id = ANY($1::uuid[]) 
          AND locked_by_server = $2
          AND is_streaming = TRUE
    """
    
    try:
        rows_updated = await self.db_manager.execute_query(
            heartbeat_query,
            (owned_stream_ids, self.server_id),
            return_rowcount=True
        )
        
        if rows_updated > 0:
            logger.debug(f"💓 Updated heartbeat for {rows_updated} cameras")
        elif len(owned_stream_ids) > 0:
            # ⚠️ WARNING: We have cameras in memory but couldn't update heartbeat
            logger.warning(
                f"⚠️ Expected to update {len(owned_stream_ids)} cameras, "
                f"but only updated {rows_updated}. Cameras may not be locked!"
            )
        
        return rows_updated
        
    except Exception as e:
        logger.error(f"Error updating heartbeat: {e}")
        return 0
</artifact>

---

## ✅ **Solution 3: Auto-Restart on Server Startup**

Add a method to reclaim orphaned cameras on startup:

<artifact identifier="startup-reclaim" type="application/vnd.ant.code" language="python" title="Startup camera reclaim function">
# Add to distributed_stream_manager.py

async def reclaim_my_cameras_on_startup(self):
    """
    🔄 Reclaim cameras that were locked by this server before restart.
    
    Called during startup to recover from unexpected shutdowns.
    """
    try:
        # Find cameras that:
        # 1. Were locked by this server_id
        # 2. Have is_streaming = TRUE
        # 3. Are not user-stopped
        
        reclaim_query = """
            SELECT 
                vs.stream_id, vs.name, vs.path, vs.workspace_id, vs.user_id,
                vs.location, vs.area, vs.building, vs.zone, vs.floor_level,
                vs.latitude, vs.longitude,
                u.username, u.role
            FROM video_stream vs
            JOIN users u ON vs.user_id = u.user_id
            JOIN workspaces w ON vs.workspace_id = w.workspace_id
            WHERE vs.locked_by_server = $1
              AND vs.is_streaming = TRUE
              AND vs.stop_reason NOT IN ('user_action', 'user_stop', 'manual_stop', 'admin_stop')
              AND vs.auto_retry_enabled = TRUE
              AND u.is_active = TRUE
              AND w.is_active = TRUE
              AND (u.is_subscribed = TRUE OR u.role = 'admin')
        """
        
        cameras_to_reclaim = await self.db_manager.execute_query(
            reclaim_query,
            (self.server_id,),
            fetch_all=True
        )
        
        if not cameras_to_reclaim:
            logger.info(f"✅ No cameras to reclaim on startup")
            return
        
        logger.warning(
            f"🔄 Found {len(cameras_to_reclaim)} cameras to reclaim from previous session"
        )
        
        # Update heartbeat to mark them as active
        update_query = """
            UPDATE video_stream
            SET server_heartbeat = NOW(),
                status = 'processing',
                updated_at = NOW()
            WHERE locked_by_server = $1
              AND is_streaming = TRUE
        """
        
        await self.db_manager.execute_query(update_query, (self.server_id,))
        
        # Log reclaimed cameras
        for camera in cameras_to_reclaim:
            logger.info(
                f"🔄 Will reclaim: {camera['name']} "
                f"(stream_id: {camera['stream_id']})"
            )
        
        logger.info(
            f"✅ Reclaimed {len(cameras_to_reclaim)} cameras. "
            f"They will be started by the management loop."
        )
        
    except Exception as e:
        logger.error(f"Error reclaiming cameras on startup: {e}", exc_info=True)


# Modify start_management_loop to call reclaim on startup
async def start_management_loop(self):
    """Start the distributed management loop."""
    if self.is_running:
        logger.warning("Management loop already running")
        return
    
    self.is_running = True
    
    # ✅ NEW: Reclaim cameras from previous session
    await self.reclaim_my_cameras_on_startup()
    
    self.management_task = asyncio.create_task(
        self.manage_streams_with_deduplication()
    )
    self.management_task.set_name(f"distributed_manager_{self.server_id}")
    
    logger.info(f"✅ Started management loop for server {self.server_id}")
</artifact>

---

## ✅ **Solution 4: Alternative - Release Orphaned Locks on Startup**

If you **don't** want to auto-restart previous cameras, release their locks instead:

<artifact identifier="release-orphans" type="application/vnd.ant.code" language="python" title="Release orphaned locks on startup">
async def release_my_orphaned_cameras_on_startup(self):
    """
    🗑️ Release cameras that were locked by this server in a previous session.
    
    Use this if you DON'T want cameras to auto-restart after server restart.
    """
    try:
        release_query = """
            UPDATE video_stream
            SET 
                locked_by_server = NULL,
                server_heartbeat = NULL,
                status = 'inactive',
                updated_at = NOW()
            WHERE locked_by_server = $1
            RETURNING stream_id, name
        """
        
        released = await self.db_manager.execute_query(
            release_query,
            (self.server_id,),
            fetch_all=True
        )
        
        if released:
            logger.warning(
                f"🗑️ Released {len(released)} orphaned cameras from previous session: "
                f"{[r['name'] for r in released]}"
            )
        else:
            logger.info("✅ No orphaned cameras to release")
        
    except Exception as e:
        logger.error(f"Error releasing orphaned cameras: {e}", exc_info=True)
</artifact>

---

## 🎯 **Recommended Implementation**

### **Step 1: Update `stream_service.py`**

Apply the **Solution 1** fix to `start_stream_background()` to claim locks for API-started cameras.

### **Step 2: Add Startup Reclaim**

Add **Solution 3** to `distributed_stream_manager.py`:

```python
# In distributed_stream_manager.py

async def start_management_loop(self):
    """Start the distributed management loop."""
    if self.is_running:
        logger.warning("Management loop already running")
        return
    
    self.is_running = True
    
    # ✅ Reclaim cameras from previous session
    await self.reclaim_my_cameras_on_startup()
    
    self.management_task = asyncio.create_task(
        self.manage_streams_with_deduplication()
    )
    self.management_task.set_name(f"distributed_manager_{self.server_id}")
    
    logger.info(f"✅ Started management loop for server {self.server_id}")
```

### **Step 3: Test**

```bash
# 1. Start a camera via API
curl -X POST http://localhost:8000/insighteye/start \
  -H "Authorization: Bearer YOUR_TOKEN"

# 2. Verify lock is set
psql -U postgres -d insighteye_db -c "
  SELECT stream_id, name, locked_by_server, server_heartbeat 
  FROM video_stream 
  WHERE is_streaming = TRUE;
"

# Should show:
# locked_by_server: 39959962-6877-4173-a7c1-8643d3256f4f
# server_heartbeat: 2026-01-10 21:56:00

# 3. Restart server
docker-compose restart app

# 4. Wait 30 seconds, then check database
psql -U postgres -d insighteye_db -c "
  SELECT stream_id, name, status, is_streaming, locked_by_server
  FROM video_stream
  WHERE name = 'DVR13 - Camera 15';
"

# Should show:
# status: 'active' or 'processing'
# is_streaming: true
# locked_by_server: <new server_id>
```

---

## 📊 **Verification Queries**

### **Check Current Locks**
```sql
SELECT 
    stream_id,
    name,
    locked_by_server,
    server_heartbeat,
    AGE(NOW(), server_heartbeat) as heartbeat_age,
    is_streaming,
    status
FROM video_stream
WHERE locked_by_server IS NOT NULL
ORDER BY server_heartbeat DESC;
```

### **Find Orphaned Cameras**
```sql
-- Cameras that should be running but aren't locked
SELECT stream_id, name, is_streaming, status, locked_by_server
FROM video_stream
WHERE is_streaming = TRUE
  AND locked_by_server IS NULL
  AND stop_reason NOT IN ('user_action', 'user_stop');
```

### **Check Stale Locks**
```sql
SELECT 
    stream_id,
    name,
    locked_by_server,
    AGE(NOW(), server_heartbeat) as heartbeat_age
FROM video_stream
WHERE locked_by_server IS NOT NULL
  AND server_heartbeat < NOW() - INTERVAL '2 minutes';
```

---

## 🚀 **Expected Behavior After Fix**

1. **API Start**: Sets `locked_by_server` immediately ✅
2. **Heartbeat Updates**: Every 30s via management loop ✅
3. **Server Restart**: Auto-reclaims cameras within 30s ✅
4. **User Stop**: Releases lock + sets `auto_retry_enabled=FALSE` ✅
5. **System Error**: Keeps lock for retry ✅

---

Apply these fixes and the cameras will be properly locked, heartbeats will update, and auto-restart will work after server restarts! 🎉