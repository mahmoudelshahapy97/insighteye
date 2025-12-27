# app/services/fire_detection_debouncer.py
"""Debouncing logic for fire detection to prevent alert spam."""

import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional
from collections import defaultdict

logger = logging.getLogger(__name__)


class FireDetectionDebouncer:
    """
    Debounces fire detection events to prevent rapid-fire alerts.
    
    Features:
    - Requires sustained detection before triggering alert
    - Prevents re-alerting for same event
    - Automatic clearing when fire is no longer detected
    """
    
    def __init__(
        self,
        detection_duration_seconds: int = 3,  # Must detect fire for 3 seconds
        cooldown_minutes: int = 10,  # Don't re-alert for 10 minutes
        clear_duration_seconds: int = 5  # Must be clear for 5 seconds to reset
    ):
        self.detection_duration = timedelta(seconds=detection_duration_seconds)
        self.cooldown_duration = timedelta(minutes=cooldown_minutes)
        self.clear_duration = timedelta(seconds=clear_duration_seconds)
        
        # Track detection state per camera
        self._first_detection: Dict[str, Optional[datetime]] = defaultdict(lambda: None)
        self._last_alert_sent: Dict[str, Optional[datetime]] = defaultdict(lambda: None)
        self._last_detection: Dict[str, Optional[datetime]] = defaultdict(lambda: None)
        self._alert_active: Dict[str, bool] = defaultdict(lambda: False)
        
    def should_send_alert(self, camera_id: str, fire_detected: bool) -> bool:
        """
        Determine if a fire alert should be sent.
        
        Args:
            camera_id: Unique identifier for the camera
            fire_detected: Whether fire is currently detected
            
        Returns:
            bool: True if alert should be sent
        """
        now = datetime.now(ZoneInfo("Africa/Cairo"))
        
        if fire_detected:
            self._last_detection[camera_id] = now
            
            # First detection for this camera
            if self._first_detection[camera_id] is None:
                self._first_detection[camera_id] = now
                logger.info(f"Fire first detected on {camera_id} at {now}")
                return False  # Don't alert yet, wait for sustained detection
            
            # Check if fire has been detected long enough
            detection_duration = now - self._first_detection[camera_id]
            
            if detection_duration < self.detection_duration:
                logger.debug(
                    f"Fire detected on {camera_id} for {detection_duration.total_seconds():.1f}s "
                    f"(need {self.detection_duration.total_seconds()}s)"
                )
                return False  # Not sustained long enough
            
            # Check cooldown period
            last_alert = self._last_alert_sent[camera_id]
            if last_alert is not None:
                time_since_alert = now - last_alert
                if time_since_alert < self.cooldown_duration:
                    remaining = (self.cooldown_duration - time_since_alert).total_seconds()
                    logger.debug(
                        f"Fire alert for {camera_id} in cooldown. "
                        f"{remaining:.0f}s remaining."
                    )
                    return False  # Still in cooldown
            
            # All conditions met - send alert
            self._last_alert_sent[camera_id] = now
            self._alert_active[camera_id] = True
            logger.warning(
                f"🔥 ALERT TRIGGERED for {camera_id} - "
                f"Fire detected for {detection_duration.total_seconds():.1f}s"
            )
            return True
            
        else:
            # Fire not detected
            last_detection = self._last_detection[camera_id]
            
            if last_detection is None:
                return False  # Never detected fire
            
            # Check if fire has been clear long enough to reset
            time_since_detection = now - last_detection
            
            if time_since_detection >= self.clear_duration:
                # Reset state
                if self._alert_active[camera_id]:
                    logger.info(
                        f"✅ Fire cleared on {camera_id} - "
                        f"Clear for {time_since_detection.total_seconds():.1f}s"
                    )
                
                self._first_detection[camera_id] = None
                self._alert_active[camera_id] = False
                # Note: We keep _last_alert_sent for cooldown tracking
            
            return False
    
    def is_alert_active(self, camera_id: str) -> bool:
        """Check if an alert is currently active for a camera."""
        return self._alert_active[camera_id]
    
    def force_reset(self, camera_id: str):
        """Force reset detection state for a camera."""
        logger.info(f"Force resetting fire detection state for {camera_id}")
        self._first_detection[camera_id] = None
        self._last_detection[camera_id] = None
        self._alert_active[camera_id] = False
        # Keep _last_alert_sent for cooldown
    
    def get_status(self, camera_id: str) -> Dict:
        """Get current status for a camera."""
        now = datetime.now(ZoneInfo("Africa/Cairo"))
        first_det = self._first_detection[camera_id]
        last_det = self._last_detection[camera_id]
        last_alert = self._last_alert_sent[camera_id]
        
        status = {
            'alert_active': self._alert_active[camera_id],
            'first_detection_time': first_det.isoformat() if first_det else None,
            'last_detection_time': last_det.isoformat() if last_det else None,
            'last_alert_time': last_alert.isoformat() if last_alert else None,
        }
        
        if first_det and last_det:
            status['detection_duration_seconds'] = (last_det - first_det).total_seconds()
        
        if last_alert:
            status['time_since_alert_seconds'] = (now - last_alert).total_seconds()
            status['cooldown_remaining_seconds'] = max(
                0, 
                (self.cooldown_duration - (now - last_alert)).total_seconds()
            )
        
        return status


# Global debouncer instance
fire_debouncer = FireDetectionDebouncer(
    detection_duration_seconds=3,  # Require 3 seconds of sustained detection
    cooldown_minutes=10,  # Don't re-alert for 10 minutes
    clear_duration_seconds=5  # Must be clear for 5 seconds to reset
)