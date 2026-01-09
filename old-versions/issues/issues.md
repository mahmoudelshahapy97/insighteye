# Step-by-Step Analysis of Your RTSP Stream Disconnection Issue

## 🔍 **Issue Summary**
Your RTSP cameras start streaming successfully, then suddenly drop, and fail to reconnect despite retry attempts.

---

## 📊 **Step 1: Identify the Pattern in Logs**

From your logs, I can see a clear pattern:

```
11:17:07 ✓ RTSP connected: 1280x1944 @ 20.0fps
11:17:12 ⚠️ RTSP failure, retry in 2s (attempt 1/5)
11:17:14 ⚠️ RTSP failure, retry in 2s (attempt 1/5)
...continues failing...
```

**Key Observations:**
1. ✅ Cameras connect successfully initially
2. ⚠️ Within 5-30 seconds, they start failing
3. 🔄 Retry attempts continue but keep failing
4. 🔁 Pattern repeats for all cameras (ch09, ch10, ch11, ch12, ch13, ch14, ch15)

---

## 🐛 **Step 2: Root Cause Analysis**

### **Issue #1: RTSP Frame Read Failures**
Looking at `shared_stream_service.py` line 406-450:

```python
# CRITICAL: Thread-safe read with proper None check
async with self._capture_lock:
    if (not self.stop_capture.is_set() and 
        self.is_running and 
        self.cap is not None and
        self.cap.isOpened()):
        
        try:
            ret, frame = await loop.run_in_executor(None, self.cap.read)
            frame_read_successful = True
```

**Problem:** The code is reading from `cv2.VideoCapture`, but RTSP streams can have:
- Network interruptions
- Codec errors
- Buffer overflows
- Connection timeouts

---

### **Issue #2: Aggressive Error Handling**
From `shared_stream_service.py` line 500:

```python
if not frame_read_successful or not ret or frame is None:
    self._handle_read_failure()
    
    if self.is_rtsp_source:
        time_since_success = time.time() - last_successful_read
        if time_since_success > read_timeout:  # 30 seconds
            logger.error(f"No frames for {time_since_success:.1f}s")
            break  # Exits loop completely
```

**Problem:** After just 30 seconds of failures, the entire capture loop exits. This is too aggressive for real-world RTSP streams.

---

### **Issue #3: Reconnection Logic Creates New Stream Objects**
From logs:
```
11:17:14 Opening RTSP stream: rtsp://admin:12345678@41.178.2.61:5513/ch12/0
11:17:16 Opening RTSP stream: rtsp://admin:12345678@41.178.2.61:5513/ch13/0
11:17:18 Opening RTSP stream: rtsp://admin:12345678@41.178.2.61:5513/ch09/0
```

**Problem:** The system creates **new RTSP connections** for each retry instead of reusing the existing stream object. This:
- Wastes network resources
- Can trigger IP camera connection limits
- Creates race conditions

---

### **Issue #4: Distributed Manager May Interfere**
From `distributed_stream_manager.py` line 90-150, the distributed manager:

1. Claims cameras every 20 seconds
2. Checks if cameras are healthy
3. Releases locks if unhealthy

**Problem:** If a camera has a temporary network glitch:
- Stream manager marks it as unhealthy
- Distributed manager releases the lock
- Another server tries to claim it
- Original server loses control
- Camera never recovers

---

### **Issue #5: Database State Conflicts**
From `stream_service.py` line 1050:

```python
# Check if database says is_streaming=FALSE (user stopped)
if not db_stream_state.get('is_streaming', False):
    logger.warning(f"🛑 Camera {stream_id_str} in memory but database says is_streaming=FALSE")
    await self._stop_stream(stream_id_str, for_restart=False)
```

**Problem:** If there's a race condition where:
1. Camera fails temporarily → `status='error'`
2. Another process updates database → `is_streaming=FALSE`
3. Stream manager sees this → Stops the camera permanently
4. Retry system can't recover because `is_streaming=FALSE`

---

## 🔧 **Step 3: Why Your Specific Cameras Fail**

### **Camera-Specific Issues:**

1. **Different Resolutions:**
   ```
   ch14: 1920x1080 @ 25fps (high resolution)
   ch11: 1280x1944 @ 20fps (very tall resolution - unusual)
   ch12: 1280x1944 @ 20fps
   ```
   
   **Problem:** The 1920x1080 cameras likely have higher bandwidth, causing network congestion. The unusual 1280x1944 resolution might have codec issues.

2. **All Using Same IP Address:**
   ```
   rtsp://admin:12345678@41.178.2.61:5513/ch{XX}/0
   ```
   
   **Problem:** All cameras share the same NVR/IP. Opening 7+ concurrent RTSP streams from one IP can:
   - Exceed the NVR's connection limit
   - Cause network congestion
   - Trigger rate limiting

---

## 🔬 **Step 4: Technical Deep Dive**

### **Why It Works Initially Then Fails:**

```
Timeline:
11:17:07 → Camera connects (initial connection succeeds)
11:17:12 → First frame read failure (5 seconds later)
11:17:14 → Retry attempt fails
11:17:16 → Another retry fails
...continues failing...
```

**Root Cause Chain:**

1. **Initial Success:** Fresh TCP connection, camera buffer is empty
2. **Frame Accumulation:** Camera keeps sending frames, buffer fills up
3. **Buffer Overflow:** After 5-30 seconds, OpenCV's internal buffer overflows
4. **Read Timeout:** `cap.read()` blocks waiting for frames
5. **Failure Cascade:** Timeout triggers `_handle_read_failure()`
6. **Stream Restart:** Code tries to reconnect but old connection still exists
7. **Port Exhaustion:** New connection fails because NVR has too many open connections
8. **Permanent Failure:** System can't recover

---

## 🎯 **Step 5: The Smoking Gun**

Look at this pattern in your logs:

```
11:25:45 ⚠️ RTSP failure, retry in 2s (attempt 1/5)
11:25:47 Opening RTSP stream: rtsp://...ch11/0
11:25:48 🛑 Signal SIGTERM received. Initiating graceful shutdown
11:25:54 ⚠️ RTSP failure, retry in 2s (attempt 1/5)
11:25:54 ✓ RTSP connected: 1280x1944 @ 20.0fps
```

**What's Happening:**
- Camera fails at 11:25:45
- Tries to reconnect at 11:25:47
- Server receives SIGTERM (shutdown signal) at 11:25:48
- But camera actually reconnects at 11:25:54!

**This proves:** The camera CAN reconnect, but timing/coordination issues cause failures.

---

## ✅ **Step 6: Recommended Fixes**

### **Fix #1: Increase Read Timeout**
```python
# In shared_stream_service.py, line 500
read_timeout = 120.0  # Change from 30.0 to 120.0 seconds
```

### **Fix #2: Better RTSP Configuration**
```python
# In shared_stream_service.py, line 280
os.environ['OPENCV_FFMPEG_CAPTURE_OPTIONS'] = (
    f'rtsp_transport;tcp|'
    f'timeout;30000000|'  # Increase from 15 to 30 seconds
    f'buffer_size;1024000|'  # Add larger buffer
    f'max_delay;5000000|'  # Increase delay tolerance
    f'analyzeduration;5000000|'  # Longer analysis
    f'probesize;5000000'  # Larger probe
)
```

### **Fix #3: Gradual Reconnection**
```python
# In shared_stream_service.py, add exponential backoff
delay = min(30.0, 2.0 * (1.5 ** (self.reconnect_attempts - 1)))
await asyncio.sleep(delay)  # Instead of fixed 2 seconds
```

### **Fix #4: Connection Pooling**
```python
# Reuse existing RTSP connections instead of creating new ones
if self.cap is not None and self.cap.isOpened():
    # Try to recover existing connection
    self.cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
    # Clear buffer by reading dummy frames
    for _ in range(10):
        self.cap.read()
```

### **Fix #5: Disable Distributed Manager Interference**
```python
# In distributed_stream_manager.py, increase grace period
if time_since_success < 300:  # Change from 60 to 300 seconds
    # Give camera more time to recover
    continue
```

---

## 🚀 **Step 7: Immediate Action Plan**

1. **Reduce Concurrent Streams:**
   - Start with 3-4 cameras max
   - Monitor stability
   - Gradually add more

2. **Increase Timeouts:**
   - RTSP timeout: 30 seconds → 60 seconds
   - Read timeout: 30 seconds → 120 seconds
   - Retry timeout: 2 seconds → 5-10 seconds (exponential)

3. **Add Monitoring:**
   ```python
   logger.info(f"📊 Buffer size: {self.cap.get(cv2.CAP_PROP_BUFFERSIZE)}")
   logger.info(f"📊 FPS: {self.cap.get(cv2.CAP_PROP_FPS)}")
   logger.info(f"📊 Frame count: {self.frame_count}")
   ```

4. **Test Camera Health:**
   ```bash
   # Test RTSP stream directly
   ffmpeg -rtsp_transport tcp -i "rtsp://admin:12345678@41.178.2.61:5513/ch14/0" -t 60 test.mp4
   ```

---

## 📝 **Conclusion**

Your issue is a **combination of**:
1. ❌ **Too aggressive error handling** (30-second timeout)
2. ❌ **Network congestion** (7 streams from same IP)
3. ❌ **Poor reconnection logic** (creates new connections instead of recovering)
4. ❌ **Distributed manager interference** (releases locks too quickly)
5. ❌ **RTSP buffer overflow** (frames accumulate faster than processed)

The fact that cameras **connect initially but fail after 5-30 seconds** points to **buffer management** as the primary issue.

Would you like me to create a complete patch file with all these fixes?