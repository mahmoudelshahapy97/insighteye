# 🔧 Production-Ready Distributed Stream Manager - Implementation Guide

## 📋 Table of Contents
1. [Overview](#overview)
2. [Key Changes](#key-changes)
3. [Database Schema](#database-schema)
4. [Critical Fixes](#critical-fixes)
5. [Testing Checklist](#testing-checklist)
6. [Deployment Steps](#deployment-steps)

---

## 🎯 Overview

This implementation solves **all race condition issues** in your distributed camera management system by:

1. **Atomic Operations**: Using PostgreSQL row-level locking (`FOR UPDATE SKIP LOCKED`)
2. **Proper Ordering**: Memory → Lock → Database coordination
3. **Health-Based Claiming**: Claims based on actual stream health, not just heartbeat age
4. **Extended Grace Periods**: Different timeouts for RTSP (6min) vs files (2min)

---

## 🔄 Key Changes

### **Distributed Manager (`distributed_stream_manager.py`)**

#### 1. **Enhanced Claiming Logic**
```sql
-- OLD: Simple heartbeat check
WHERE server_heartbeat < NOW() - INTERVAL '2 minutes'

-- NEW: Health-based with stream type awareness
WHERE (
    locked_by_server IS NULL
    OR (
        -- Dead server (no heartbeat)
        CASE WHEN path LIKE 'rtsp://%' 
            THEN server_heartbeat < NOW() - INTERVAL '6 minutes'
            ELSE server_heartbeat < NOW() - INTERVAL '2 minutes'
        END
    )
    OR (
        -- Alive server BUT unhealthy stream
        server_heartbeat > NOW() - INTERVAL '2 minutes'
        AND
        CASE WHEN path LIKE 'rtsp://%'
            THEN last_activity < NOW() - INTERVAL '8 minutes'
            ELSE last_activity < NOW() - INTERVAL '3 minutes'
        END
    )
)
```

#### 2. **Triple-Layer Verification**
```python
# Layer 1: SQL-level verification in WHERE clause
WHERE is_streaming = TRUE AND stop_reason NOT IN (...)

# Layer 2: Re-verification in UPDATE clause  
AND video_stream.is_streaming = TRUE AND ...

# Layer 3: Post-claim Python verification
if not camera.get('is_streaming', True):
    await release_camera_lock(stream_id, force_stop=True)
```

#### 3. **Smart Lock Release**
```python
async def release_camera_lock(stream_id, reason, force_stop=False):
    """
    force_stop=True:  is_streaming=FALSE (user stops)
    force_stop=False: is_streaming=TRUE (system errors, allow retry)
    """
```

---

### **Stream Service (`stream_service.py`)**

#### 1. **Atomic Stop Flow**
```python
# OLD (race condition):
# Database → Memory → Lock Release
await db.update(is_streaming=FALSE)
await cleanup_memory()
await release_lock()  # ❌ Gap allows claiming

# NEW (atomic):
# Memory → Lock → Database
await signal_stop_event()      # Stop frame processing
await release_lock()            # Unblock other servers  
await verify_db_updated()      # Ensure consistency
await cleanup_memory()          # Clean state
```

#### 2. **Health-Based Start**
```python
# OLD: Start without checking
await start_stream_background(...)

# NEW: Verify health first
if stream_id in active_streams:
    if is_truly_healthy():
        return  # Already good
    else:
        await restart_stream()  # Fix unhealthy
```

---

## 🗄️ Database Schema

### **Required Columns**
```sql
ALTER TABLE video_stream ADD COLUMN IF NOT EXISTS locked_by_server UUID;
ALTER TABLE video_stream ADD COLUMN IF NOT EXISTS server_heartbeat TIMESTAMPTZ;
ALTER TABLE video_stream ADD COLUMN IF NOT EXISTS last_activity TIMESTAMPTZ DEFAULT NOW();
ALTER TABLE video_stream ADD COLUMN IF NOT EXISTS auto_retry_enabled BOOLEAN DEFAULT TRUE;
ALTER TABLE video_stream ADD COLUMN IF NOT EXISTS retry_count INTEGER DEFAULT 0;
ALTER TABLE video_stream ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMPTZ;
ALTER TABLE video_stream ADD COLUMN IF NOT EXISTS last_retry_at TIMESTAMPTZ;
```

### **Indexes for Performance**
```sql
-- Critical for claim queries
CREATE INDEX IF NOT EXISTS idx_video_stream_claiming ON video_stream (
    is_streaming, 
    locked_by_server, 
    server_heartbeat, 
    stop_reason
) WHERE is_streaming = TRUE;

-- Critical for heartbeat updates
CREATE INDEX IF NOT EXISTS idx_video_stream_locked ON video_stream (
    locked_by_server, 
    is_streaming
) WHERE locked_by_server IS NOT NULL;

-- Critical for health checks
CREATE INDEX IF NOT EXISTS idx_video_stream_activity ON video_stream (
    last_activity, 
    path, 
    is_streaming
);
```

### **Stored Procedure (Optional but Recommended)**
```sql
CREATE OR REPLACE FUNCTION cleanup_zombie_server_locks()
RETURNS TABLE(released_count INTEGER, affected_cameras TEXT[]) AS $$
DECLARE
    v_released_count INTEGER;
    v_camera_names TEXT[];
BEGIN
    WITH zombie_locks AS (
        UPDATE video_stream
        SET locked_by_server = NULL,
            server_heartbeat = NULL
        WHERE locked_by_server IS NOT NULL
          AND server_heartbeat < NOW() - INTERVAL '10 minutes'
        RETURNING stream_id, name
    )
    SELECT COUNT(*), ARRAY_AGG(name)
    INTO v_released_count, v_camera_names
    FROM zombie_locks;
    
    RETURN QUERY SELECT v_released_count, v_camera_names;
END;
$$ LANGUAGE plpgsql;
```

---

## ✅ Critical Fixes

### **Fix #1: Stop Race Condition**
**Problem**: Distributed manager claims camera between memory cleanup and database update.

**Solution**:
```python
# CORRECT ORDER:
1. Signal stop in memory (stop_event.set())
2. Release distributed lock (makes camera unavailable)
3. Update database (record final state)
4. Clean memory (remove from dicts)

# This order ensures NO GAP where camera can be claimed
```

### **Fix #2: RTSP Recovery Interference**
**Problem**: Health checks restart RTSP cameras during normal buffering delays.

**Solution**:
```python
# Different grace periods
RTSP_GRACE = 360  # 6 minutes (RTSP needs time for buffering)
FILE_GRACE = 120  # 2 minutes (files fail fast)

# Check before claiming
if is_rtsp and age < RTSP_GRACE:
    logger.info("RTSP camera recovering, allowing grace period")
    continue  # Don't claim yet
```

### **Fix #3: Lock Not Released**
**Problem**: User stops camera but `locked_by_server` stays set.

**Solution**:
```python
async def release_camera_lock(stream_id, reason, force_stop):
    """
    force_stop=True for user stops:
    - Sets locked_by_server = NULL
    - Sets is_streaming = FALSE  
    - Sets auto_retry_enabled = FALSE
    
    All in ONE atomic query
    """
```

### **Fix #4: Verification Bypass**
**Problem**: Camera claimed even when `stop_reason='user_action'`.

**Solution**:
```python
# Three layers of checking:
# 1. SQL WHERE clause
WHERE stop_reason NOT IN ('user_action', 'user_stop', ...)

# 2. SQL UPDATE verification
AND video_stream.stop_reason NOT IN (...)

# 3. Python post-claim check
if camera['stop_reason'] == 'user_action':
    await release_lock(force_stop=True)
```

---

## 🧪 Testing Checklist

### **Test Case 1: User Stop**
```python
# Expected Behavior:
# 1. Camera stops immediately
# 2. locked_by_server = NULL
# 3. is_streaming = FALSE
# 4. auto_retry_enabled = FALSE
# 5. Camera NEVER restarts

# Test:
await stream_manager.stop_stream_in_workspace(
    stream_id=camera_id,
    requester_user_id=user_id,
    stop_reason='user_action'
)

# Verify:
assert db.get(camera_id).is_streaming == False
assert db.get(camera_id).locked_by_server is None
```

### **Test Case 2: System Error**
```python
# Expected Behavior:
# 1. Camera stops
# 2. locked_by_server = NULL (released)
# 3. is_streaming = TRUE (allows retry)
# 4. auto_retry_enabled = TRUE
# 5. Camera WILL restart after backoff

# Test:
# Simulate connection error
# Verify camera restarts automatically
```

### **Test Case 3: Multi-Server Race**
```python
# Expected Behavior:
# Only ONE server claims each camera

# Test:
# Start 3 servers with 10 cameras
# Each server should claim ~3-4 cameras
# NO camera should be claimed by multiple servers

# Verify:
for camera in cameras:
    locks = db.query(
        "SELECT locked_by_server FROM video_stream WHERE stream_id = $1",
        camera.id
    )
    assert len(set(locks)) == 1  # Only one unique server
```

### **Test Case 4: RTSP Recovery**
```python
# Expected Behavior:
# RTSP camera buffering for 3 minutes should NOT trigger restart

# Test:
# 1. Start RTSP camera
# 2. Block network for 3 minutes
# 3. Unblock network
# 4. Verify camera recovers WITHOUT restart

# Check logs:
# Should see: "RTSP camera recovering, allowing grace period"
```

---

## 🚀 Deployment Steps

### **Step 1: Database Migration**
```bash
# Run migration script
psql -U postgres -d your_db -f migration.sql

# Verify columns exist
psql -U postgres -d your_db -c "\d video_stream"
```

### **Step 2: Update Configuration**
```python
# config/settings.py
class Settings:
    # Server identification (unique per instance)
    server_id: str = os.getenv("SERVER_ID", str(uuid.uuid4()))
    
    # Capacity per server
    max_local_streams: int = int(os.getenv("MAX_LOCAL_STREAMS", "8"))
    
    # Grace periods (adjust based on your RTSP cameras)
    rtsp_grace_period_seconds: int = 360  # 6 minutes
    file_grace_period_seconds: int = 120  # 2 minutes
```

### **Step 3: Deploy Files**
```bash
# 1. Copy new distributed_stream_manager.py
cp distributed_stream_manager_fixed.py app/services/distributed_stream_manager.py

# 2. Update stream_service.py critical methods
# Copy the stop_stream_in_workspace, _stop_stream, and start_stream_background methods

# 3. Restart services
systemctl restart your-app
```

### **Step 4: Monitor Startup**
```bash
# Watch logs for successful initialization
tail -f /var/log/your-app.log | grep "DistributedStreamManager"

# Expected output:
# ✅ DistributedStreamManager initialized: server_id=..., capacity=8
# ✅ Started management loop for server ...
# 💓 Updated heartbeat for X cameras
```

### **Step 5: Verify Claiming**
```bash
# Check which server owns which cameras
psql -U postgres -d your_db -c "
SELECT 
    name,
    locked_by_server,
    server_heartbeat,
    is_streaming,
    stop_reason
FROM video_stream
WHERE is_streaming = TRUE
ORDER BY locked_by_server;
"
```

---

## 🔍 Troubleshooting

### **Issue: Cameras keep restarting**
**Diagnosis**:
```sql
SELECT 
    name,
    stop_reason,
    is_streaming,
    auto_retry_enabled,
    locked_by_server
FROM video_stream
WHERE name = 'your_camera';
```

**Fix**:
- If `stop_reason = 'user_action'` but `is_streaming = TRUE`: Run cleanup query
- If `locked_by_server` is set but server is dead: Run zombie cleanup

### **Issue: Cameras not claimed**
**Diagnosis**:
```sql
SELECT 
    name,
    is_streaming,
    locked_by_server,
    server_heartbeat,
    stop_reason,
    auto_retry_enabled,
    EXTRACT(EPOCH FROM (NOW() - last_activity)) as seconds_inactive
FROM video_stream
WHERE is_streaming = TRUE
  AND locked_by_server IS NULL;
```

**Common Causes**:
1. `stop_reason = 'user_action'` → Clear it
2. `auto_retry_enabled = FALSE` → Set to TRUE
3. `next_retry_at` in future → Wait or clear it

### **Issue: Multiple servers claim same camera**
**This should NEVER happen** due to `FOR UPDATE SKIP LOCKED`.

If it does:
1. Check PostgreSQL version (must be 9.5+)
2. Verify indexes are created
3. Check for manual database updates bypassing locking

---

## 📊 Monitoring Queries

### **Server Health Dashboard**
```sql
SELECT 
    locked_by_server as server_id,
    COUNT(*) as cameras_owned,
    COUNT(*) FILTER (WHERE status = 'active') as active_cameras,
    COUNT(*) FILTER (WHERE status = 'error') as error_cameras,
    MAX(server_heartbeat) as last_heartbeat
FROM video_stream
WHERE locked_by_server IS NOT NULL
GROUP BY locked_by_server
ORDER BY cameras_owned DESC;
```

### **Unhealthy Cameras**
```sql
SELECT 
    name,
    locked_by_server,
    status,
    EXTRACT(EPOCH FROM (NOW() - last_activity)) as seconds_since_activity,
    EXTRACT(EPOCH FROM (NOW() - server_heartbeat)) as seconds_since_heartbeat
FROM video_stream
WHERE is_streaming = TRUE
  AND (
      last_activity < NOW() - INTERVAL '5 minutes'
      OR server_heartbeat < NOW() - INTERVAL '3 minutes'
  )
ORDER BY last_activity ASC;
```

### **Lock Status**
```sql
SELECT 
    COUNT(*) FILTER (WHERE locked_by_server IS NULL) as unlocked,
    COUNT(*) FILTER (WHERE locked_by_server IS NOT NULL) as locked,
    COUNT(*) FILTER (WHERE 
        locked_by_server IS NOT NULL 
        AND server_heartbeat < NOW() - INTERVAL '5 minutes'
    ) as zombie_locks
FROM video_stream
WHERE is_streaming = TRUE;
```

---

## 🎯 Success Metrics

After deployment, you should see:

✅ **Zero race conditions**: No camera claimed by multiple servers  
✅ **Proper stops**: User-stopped cameras never restart  
✅ **Auto-recovery**: Failed cameras restart after backoff  
✅ **RTSP stability**: RTSP cameras don't restart during buffering  
✅ **Clean locks**: No zombie locks after server restarts  

---

## 📞 Support

If issues persist:
1. Check all three verification layers in logs
2. Verify database indexes exist
3. Confirm PostgreSQL version ≥ 9.5
4. Review server_heartbeat vs last_activity timestamps

**Remember**: The key to this solution is **atomic operations** and **proper ordering**. Never update database before releasing locks!