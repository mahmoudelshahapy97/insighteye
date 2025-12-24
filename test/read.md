# RTSP Stream Troubleshooting & Configuration Guide

## Quick Fixes Applied

### 1. Reduced Timeouts (Most Important)
```python
# In shared_stream.py _get_opencv_capture_options()
'timeout;10000000|'   # 10 seconds (was 30s)
'stimeout;2000000|'   # 2 seconds (was 5s)
```

**Why**: Your logs show 30+ second hangs. Shorter timeouts fail faster and allow quicker reconnection attempts.

### 2. Improved Shutdown Sequence
```python
# Force-release VideoCapture BEFORE joining thread
if self.is_rtsp and self.cap:
    self.cap.release()  # Unblocks cap.read()
    self.cap = None
    time.sleep(0.3)     # Give thread time to detect

# Then join with shorter timeout
self.capture_thread.join(timeout=5.0)
```

**Why**: Threads get stuck on blocking `cap.read()`. Releasing first unblocks them.

---

## Diagnosing Your Specific Issue

### Stream: `rtsp://admin:12345678@41.178.2.61:554/ch12/0`

**Symptoms**:
- ❌ 30+ second timeouts
- ❌ "Cannot read from video source" errors
- ❌ Threads not stopping cleanly

**Possible Causes**:

#### 1. Network Issues (Most Likely)
```bash
# Test connectivity from your server
ping 41.178.2.61

# Test RTSP port
nc -zv 41.178.2.61 554

# Test with FFmpeg directly
ffmpeg -rtsp_transport tcp -i "rtsp://admin:12345678@41.178.2.61:554/ch12/0" \
  -frames:v 1 -f null - 2>&1 | head -20
```

#### 2. Camera Overload
- You're trying to connect to 47 cameras (16 + 16 + 15)
- **ch12** on port 554 might be failing specifically
- Other cameras on same port might work

#### 3. Bandwidth Saturation
```python
# Check if specific ports/cameras fail more
# Port 5511: 16 cameras
# Port 554:  16 cameras  ← ch12 is here
# Port 5513: 15 cameras
```

---

## Immediate Actions

### Step 1: Test Individual Streams

Create a test script:

```python
# test_single_rtsp.py
import cv2
import time
import logging

logging.basicConfig(level=logging.INFO)

def test_rtsp_stream(url, timeout=10):
    """Test a single RTSP stream"""
    print(f"\n{'='*60}")
    print(f"Testing: {url}")
    print(f"{'='*60}")
    
    # Set timeout environment variable
    import os
    os.environ['OPENCV_FFMPEG_CAPTURE_OPTIONS'] = (
        f'rtsp_transport;tcp|timeout;{timeout*1000000}|stimeout;{timeout*1000000}'
    )
    
    start_time = time.time()
    cap = cv2.VideoCapture(url, cv2.CAP_FFMPEG)
    open_time = time.time() - start_time
    
    if not cap.isOpened():
        print(f"❌ FAILED to open ({open_time:.1f}s)")
        return False
    
    print(f"✓ Opened ({open_time:.1f}s)")
    
    # Try reading frame
    start_time = time.time()
    ret, frame = cap.read()
    read_time = time.time() - start_time
    
    if ret and frame is not None:
        h, w = frame.shape[:2]
        print(f"✓ Read frame ({read_time:.1f}s): {w}x{h}")
        success = True
    else:
        print(f"❌ FAILED to read frame ({read_time:.1f}s)")
        success = False
    
    cap.release()
    return success

# Test the problematic stream
problem_url = "rtsp://admin:12345678@41.178.2.61:554/ch12/0"
test_rtsp_stream(problem_url, timeout=10)

# Test a working stream for comparison
working_url = "rtsp://admin:12345678@41.178.2.61:554/ch01/0"
test_rtsp_stream(working_url, timeout=10)
```

### Step 2: Check Camera Health

```bash
# Use VLC or ffprobe to test stream directly
ffprobe -rtsp_transport tcp \
  "rtsp://admin:12345678@41.178.2.61:554/ch12/0" \
  2>&1 | grep -E "(Stream|Duration|Video)"
```

### Step 3: Monitor Network Traffic

```bash
# Check if camera is responding
tcpdump -i any host 41.178.2.61 and port 554 -c 20
```

---

## Configuration Recommendations

### Option A: Aggressive Fast-Fail (Recommended)

```python
# config.py or environment variables
RTSP_SETTINGS = {
    "timeout": 10,              # 10 seconds max
    "stimeout": 2,              # 2 seconds socket timeout
    "max_retries": 3,           # Try 3 times then blacklist
    "blacklist_duration": 300,  # 5 minute blacklist
    "health_check_interval": 15 # Check every 15s
}
```

**Pros**: Failed streams don't block working ones
**Cons**: Might give up on temporarily slow cameras

### Option B: Patient Retry (Current Approach)

```python
RTSP_SETTINGS = {
    "timeout": 30,              # 30 seconds max
    "stimeout": 5,              # 5 seconds socket timeout
    "max_retries": 5,           # Try 5 times
    "blacklist_duration": 600,  # 10 minute blacklist
    "health_check_interval": 30 # Check every 30s
}
```

**Pros**: Won't give up on slow but working cameras
**Cons**: Hangs can delay other operations

### Option C: Hybrid (Best for 47 cameras)

```python
RTSP_SETTINGS = {
    "initial_timeout": 15,       # 15s for first connection
    "retry_timeout": 10,         # 10s for retries
    "stimeout": 3,               # 3s socket timeout
    "max_retries": 3,            # Quick failure
    "retry_delay": "exponential", # 2s, 4s, 8s
    "blacklist_duration": 300,   # 5 minutes
    "health_check_interval": 20  # Check every 20s
}
```

---

## Monitoring Dashboard

Add this endpoint to track stream health:

```python
@router.get("/streams/health")
async def get_streams_health(
    current_user: Dict = Depends(session_manager_global.get_current_user_full_data_dependency)
):
    """Get real-time stream health status"""
    
    health_data = {
        "total_streams": 0,
        "healthy": 0,
        "unhealthy": 0,
        "blacklisted": 0,
        "by_port": {
            "5511": {"total": 0, "healthy": 0, "failed": 0},
            "554": {"total": 0, "healthy": 0, "failed": 0},
            "5513": {"total": 0, "healthy": 0, "failed": 0}
        },
        "problem_cameras": []
    }
    
    # Get stats from video_file_manager
    stats = stream_manager.video_file_manager.get_all_stats()
    
    for source, stream_stats in stats.items():
        if not source.startswith('rtsp://'):
            continue
        
        health_data["total_streams"] += 1
        
        # Extract port from URL
        port = "unknown"
        if ":5511/" in source:
            port = "5511"
        elif ":554/" in source:
            port = "554"
        elif ":5513/" in source:
            port = "5513"
        
        # Check health
        is_healthy = (
            stream_stats['is_running'] and
            stream_stats.get('error_count', 0) < 5 and
            time.time() - stream_stats.get('last_frame_time', 0) < 60
        )
        
        if port in health_data["by_port"]:
            health_data["by_port"][port]["total"] += 1
            if is_healthy:
                health_data["healthy"] += 1
                health_data["by_port"][port]["healthy"] += 1
            else:
                health_data["unhealthy"] += 1
                health_data["by_port"][port]["failed"] += 1
                
                # Extract camera name
                camera_name = source.split('/')[-2] if '/' in source else "unknown"
                health_data["problem_cameras"].append({
                    "source": source,
                    "camera": camera_name,
                    "port": port,
                    "error_count": stream_stats.get('error_count', 0),
                    "last_error": stream_stats.get('last_error'),
                    "subscribers": stream_stats.get('subscriber_count', 0)
                })
    
    return health_data
```

---

## Expected Behavior After Fix

### Before:
```
[ WARN:0@52.989] Stream timeout triggered after 30021.503084 ms
ERROR:root:Capture thread did not stop within 10.0s timeout
```

### After:
```
INFO:root:Opening RTSP stream with TCP transport and 10s timeout
INFO:root:✓ Successfully opened in 2.3s
WARNING:root:Stream ch12 timeout after 10.0s, retrying...
INFO:root:Capture thread stopped cleanly in 0.8s
```

---

## Debugging Commands

### Check all camera channels:
```bash
# Test each channel on port 554
for ch in {01..16}; do
  echo "Testing ch$ch..."
  timeout 15 ffprobe -rtsp_transport tcp \
    "rtsp://admin:12345678@41.178.2.61:554/ch$ch/0" \
    2>&1 | grep -q "Video:" && echo "✓ ch$ch OK" || echo "✗ ch$ch FAIL"
done
```

### Monitor specific problematic stream:
```bash
# Watch ch12 specifically
while true; do
  echo "$(date): Testing ch12..."
  timeout 15 ffmpeg -rtsp_transport tcp \
    -i "rtsp://admin:12345678@41.178.2.61:554/ch12/0" \
    -frames:v 1 -f null - 2>&1 | tail -5
  sleep 10
done
```

---

## Next Steps

1. **Apply the fixes** from the updated `shared_stream.py`
2. **Test with single stream** first (ch12 specifically)
3. **Gradually add streams** (don't start all 47 at once)
4. **Monitor logs** for timeout patterns
5. **Adjust timeouts** based on your network conditions

The key insight: **Your network to camera ch12 on port 554 is slow/unstable**. The fix makes the system detect and handle this gracefully instead of hanging.