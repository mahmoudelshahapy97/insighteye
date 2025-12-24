# rtsp_health_monitor.py - Add to your project
"""
Advanced RTSP Stream Health Monitoring and Auto-Recovery System

This module monitors RTSP stream health and automatically handles problematic streams
by detecting timeouts, connection failures, and implementing smart retry logic.
"""

import asyncio
import time
import logging
from typing import Dict, Optional, Any
from datetime import datetime, timedelta
from collections import defaultdict

class RTSPHealthMonitor:
    """Monitor and auto-recover problematic RTSP streams"""
    
    def __init__(self, video_file_manager, db_manager):
        self.video_file_manager = video_file_manager
        self.db_manager = db_manager
        
        # Health tracking
        self.stream_health: Dict[str, Dict[str, Any]] = {}
        self.problem_streams: Dict[str, int] = defaultdict(int)  # source -> failure count
        self.blacklisted_streams: Dict[str, float] = {}  # source -> blacklist_until_timestamp
        
        # Thresholds
        self.timeout_threshold = 30.0  # If no frames for 30s, consider unhealthy
        self.max_consecutive_failures = 3  # Blacklist after 3 failures
        self.blacklist_duration = 300.0  # 5 minutes
        self.health_check_interval = 15.0  # Check every 15 seconds
        
        # Monitoring task
        self.monitoring_task: Optional[asyncio.Task] = None
        self.is_monitoring = False
        
        logging.info("RTSP Health Monitor initialized")
    
    async def start_monitoring(self):
        """Start the health monitoring loop"""
        if self.is_monitoring:
            return
        
        self.is_monitoring = True
        self.monitoring_task = asyncio.create_task(self._monitoring_loop())
        logging.info("RTSP Health Monitor started")
    
    async def stop_monitoring(self):
        """Stop the health monitoring loop"""
        self.is_monitoring = False
        if self.monitoring_task and not self.monitoring_task.done():
            self.monitoring_task.cancel()
            try:
                await asyncio.wait_for(self.monitoring_task, timeout=5.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass
        logging.info("RTSP Health Monitor stopped")
    
    def is_stream_blacklisted(self, source: str) -> bool:
        """Check if a stream is currently blacklisted"""
        if source not in self.blacklisted_streams:
            return False
        
        blacklist_until = self.blacklisted_streams[source]
        if time.time() < blacklist_until:
            return True
        
        # Blacklist expired, remove it
        del self.blacklisted_streams[source]
        self.problem_streams[source] = 0
        logging.info(f"Stream {source} removed from blacklist")
        return False
    
    def blacklist_stream(self, source: str, reason: str = "repeated failures"):
        """Temporarily blacklist a problematic stream"""
        blacklist_until = time.time() + self.blacklist_duration
        self.blacklisted_streams[source] = blacklist_until
        
        logging.warning(
            f"Stream {source} BLACKLISTED for {self.blacklist_duration/60:.1f} minutes. "
            f"Reason: {reason}. Will retry after: "
            f"{datetime.fromtimestamp(blacklist_until).strftime('%H:%M:%S')}"
        )
    
    def record_stream_failure(self, source: str, error: str):
        """Record a stream failure and potentially blacklist it"""
        self.problem_streams[source] += 1
        failure_count = self.problem_streams[source]
        
        logging.warning(
            f"Stream failure recorded for {source}: {error}. "
            f"Consecutive failures: {failure_count}/{self.max_consecutive_failures}"
        )
        
        if failure_count >= self.max_consecutive_failures:
            self.blacklist_stream(source, f"{failure_count} consecutive failures")
    
    def record_stream_success(self, source: str):
        """Record successful stream operation"""
        if source in self.problem_streams:
            previous_failures = self.problem_streams[source]
            if previous_failures > 0:
                logging.info(f"Stream {source} recovered after {previous_failures} failures")
            self.problem_streams[source] = 0
    
    async def _monitoring_loop(self):
        """Main monitoring loop"""
        while self.is_monitoring:
            try:
                await self._check_all_streams()
                await asyncio.sleep(self.health_check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"Error in health monitoring loop: {e}", exc_info=True)
                await asyncio.sleep(5.0)
    
    async def _check_all_streams(self):
        """Check health of all active shared streams"""
        if not hasattr(self.video_file_manager, 'shared_streams'):
            return
        
        current_time = time.time()
        
        for source, shared_stream in list(self.video_file_manager.shared_streams.items()):
            try:
                # Skip non-RTSP streams
                if not source.startswith('rtsp://'):
                    continue
                
                # Get stream stats
                stats = shared_stream.get_stats()
                
                # Check if stream is running but not producing frames
                if stats['is_running']:
                    last_frame_time = stats.get('last_frame_time', 0)
                    time_since_last_frame = current_time - last_frame_time
                    
                    if time_since_last_frame > self.timeout_threshold:
                        logging.warning(
                            f"🔴 UNHEALTHY STREAM: {source} - "
                            f"No frames for {time_since_last_frame:.1f}s. "
                            f"Subscribers: {stats['subscriber_count']}, "
                            f"Reconnect attempts: {stats['reconnect_attempts']}"
                        )
                        
                        # Record failure and potentially blacklist
                        self.record_stream_failure(
                            source, 
                            f"No frames for {time_since_last_frame:.1f}s"
                        )
                        
                        # Force restart if stream is stuck
                        if stats['subscriber_count'] > 0:
                            logging.info(f"Attempting forced restart of {source}")
                            await self._force_restart_stream(source, shared_stream)
                    else:
                        # Stream is healthy
                        self.record_stream_success(source)
                
                # Check for streams with errors
                if stats.get('error_count', 0) > 10:
                    logging.warning(
                        f"⚠️ Stream {source} has {stats['error_count']} errors. "
                        f"Last error: {stats.get('last_error')}"
                    )
            
            except Exception as e:
                logging.error(f"Error checking stream {source}: {e}")
    
    async def _force_restart_stream(self, source: str, shared_stream):
        """Force restart a problematic stream"""
        try:
            # Get list of affected stream IDs
            affected_streams = list(shared_stream.subscribers.keys())
            
            logging.info(f"Force restarting {source}. Affected streams: {affected_streams}")
            
            # Stop the shared stream
            shared_stream._stop_capture()
            
            # Wait for cleanup
            await asyncio.sleep(2.0)
            
            # The stream will automatically restart when subscribers reconnect
            # or when the next frame is requested
            
            logging.info(f"Force restart initiated for {source}")
            
        except Exception as e:
            logging.error(f"Error force restarting {source}: {e}")
            self.record_stream_failure(source, f"Restart failed: {e}")
    
    def get_health_report(self) -> Dict[str, Any]:
        """Get comprehensive health report"""
        report = {
            "timestamp": datetime.now().isoformat(),
            "blacklisted_streams": {},
            "problem_streams": {},
            "healthy_streams": []
        }
        
        # Blacklisted streams
        for source, until_timestamp in self.blacklisted_streams.items():
            remaining = until_timestamp - time.time()
            if remaining > 0:
                report["blacklisted_streams"][source] = {
                    "remaining_seconds": remaining,
                    "retry_after": datetime.fromtimestamp(until_timestamp).isoformat()
                }
        
        # Problem streams (not yet blacklisted)
        for source, failure_count in self.problem_streams.items():
            if failure_count > 0 and source not in self.blacklisted_streams:
                report["problem_streams"][source] = {
                    "consecutive_failures": failure_count,
                    "threshold": self.max_consecutive_failures
                }
        
        # Healthy streams
        if hasattr(self.video_file_manager, 'shared_streams'):
            for source, shared_stream in self.video_file_manager.shared_streams.items():
                if source.startswith('rtsp://'):
                    stats = shared_stream.get_stats()
                    if (stats['is_running'] and 
                        source not in self.blacklisted_streams and 
                        self.problem_streams.get(source, 0) == 0):
                        report["healthy_streams"].append({
                            "source": source,
                            "subscribers": stats['subscriber_count'],
                            "frame_count": stats['frame_count']
                        })
        
        return report


# Integration with stream_one.py
# Add this to your StreamManager class:

class StreamManagerWithHealthMonitor:
    """Enhanced StreamManager with RTSP health monitoring"""
    
    def __init__(self):
        # ... existing initialization ...
        self.rtsp_health_monitor = RTSPHealthMonitor(
            self.video_file_manager, 
            self.db_manager
        )
    
    async def start_background_tasks(self):
        """Start background tasks including health monitor"""
        # ... existing code ...
        await self.rtsp_health_monitor.start_monitoring()
        logging.info("RTSP Health Monitor started")
    
    async def stop_background_tasks(self):
        """Stop background tasks including health monitor"""
        await self.rtsp_health_monitor.stop_monitoring()
        # ... existing code ...
    
    async def shutdown(self):
        """Shutdown with health monitor cleanup"""
        await self.rtsp_health_monitor.stop_monitoring()
        # ... existing code ...
    
    async def _validate_stream_source(self, source: str) -> bool:
        """Enhanced validation with blacklist check"""
        # Check if stream is blacklisted
        if source.startswith('rtsp://'):
            if self.rtsp_health_monitor.is_stream_blacklisted(source):
                remaining = (
                    self.rtsp_health_monitor.blacklisted_streams[source] - 
                    time.time()
                )
                logging.warning(
                    f"Stream {source} is blacklisted. "
                    f"Retry in {remaining/60:.1f} minutes"
                )
                return False
        
        # Proceed with normal validation
        # ... existing validation code ...
    
    async def get_rtsp_health_status(self) -> Dict[str, Any]:
        """Get RTSP health status (for API endpoint)"""
        return self.rtsp_health_monitor.get_health_report()


# API Endpoint for monitoring (add to stream_one.py)
"""
@router.get("/rtsp/health")
async def get_rtsp_health(
    current_user_data: Dict = Depends(session_manager_global.get_current_user_full_data_dependency)
):
    '''Get RTSP stream health status'''
    # Check if user is admin
    if current_user_data.get('role') != 'admin':
        raise HTTPException(status_code=403, detail="Admin access required")
    
    health_report = await stream_manager.get_rtsp_health_status()
    return JSONResponse(content=health_report)
"""