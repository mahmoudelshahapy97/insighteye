# app/utils/email_digest.py
"""Email digest system for batching alerts."""

import logging
import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Any
from collections import defaultdict

logger = logging.getLogger(__name__)

# Store pending alerts
_pending_alerts = defaultdict(list)
_digest_tasks = {}


class AlertDigest:
    """Manages batching of alerts into digest emails."""
    
    def __init__(self, digest_interval_minutes: int = 15):
        self.digest_interval_minutes = digest_interval_minutes
        
    async def add_alert(
        self, 
        user_email: str, 
        alert_type: str, 
        camera_name: str, 
        details: Dict[str, Any]
    ):
        """Add an alert to the pending digest."""
        alert_key = user_email
        
        alert_data = {
            'type': alert_type,
            'camera': camera_name,
            'details': details,
            'timestamp': datetime.now(ZoneInfo("Africa/Cairo"))
        }
        
        _pending_alerts[alert_key].append(alert_data)
        
        # Start digest task if not already running
        if alert_key not in _digest_tasks:
            task = asyncio.create_task(self._schedule_digest(alert_key))
            _digest_tasks[alert_key] = task
            
        logger.info(
            f"Alert added to digest for {user_email}. "
            f"Total pending: {len(_pending_alerts[alert_key])}"
        )
    
    async def _schedule_digest(self, user_email: str):
        """Schedule sending of digest after interval."""
        try:
            # Wait for digest interval
            await asyncio.sleep(self.digest_interval_minutes * 60)
            
            # Send digest
            await self._send_digest(user_email)
            
        finally:
            # Clean up
            if user_email in _digest_tasks:
                del _digest_tasks[user_email]
    
    async def _send_digest(self, user_email: str):
        """Send accumulated alerts as a single digest email."""
        from app.utils.email_utils import send_email
        
        alerts = _pending_alerts.pop(user_email, [])
        
        if not alerts:
            return
        
        # Group alerts by type and camera
        fire_alerts = defaultdict(list)
        count_alerts = defaultdict(list)
        
        for alert in alerts:
            if alert['type'] == 'fire':
                fire_alerts[alert['camera']].append(alert)
            elif alert['type'] == 'people_count':
                count_alerts[alert['camera']].append(alert)
        
        # Build digest email
        subject = f"🚨 InsightEye Alert Digest - {len(alerts)} Events"
        
        body_parts = [
            f"Alert Digest Summary",
            f"Period: Last {self.digest_interval_minutes} minutes",
            f"Total Events: {len(alerts)}",
            f"\n"
        ]
        
        html_parts = [
            '<html><body style="font-family: Arial, sans-serif;">',
            '<h1>🚨 InsightEye Alert Digest</h1>',
            f'<p><strong>Period:</strong> Last {self.digest_interval_minutes} minutes</p>',
            f'<p><strong>Total Events:</strong> {len(alerts)}</p>',
        ]
        
        # Fire alerts section
        if fire_alerts:
            body_parts.append("🔥 FIRE/SMOKE ALERTS:")
            html_parts.append('<h2 style="color: #dc3545;">🔥 Fire/Smoke Alerts</h2><ul>')
            
            for camera, camera_alerts in fire_alerts.items():
                count = len(camera_alerts)
                last_alert = camera_alerts[-1]
                time_str = last_alert['timestamp'].strftime("%H:%M:%S UTC")
                
                body_parts.append(f"  • {camera}: {count} event(s) - Last at {time_str}")
                html_parts.append(
                    f'<li><strong>{camera}:</strong> {count} event(s) '
                    f'- Last at {time_str}</li>'
                )
            
            html_parts.append('</ul>')
            body_parts.append("")
        
        # People count alerts section
        if count_alerts:
            body_parts.append("👥 OCCUPANCY ALERTS:")
            html_parts.append('<h2 style="color: #fd7e14;">👥 Occupancy Alerts</h2><ul>')
            
            for camera, camera_alerts in count_alerts.items():
                count = len(camera_alerts)
                last_alert = camera_alerts[-1]
                time_str = last_alert['timestamp'].strftime("%H:%M:%S UTC")
                person_count = last_alert['details'].get('person_count', 'N/A')
                
                body_parts.append(
                    f"  • {camera}: {count} event(s) - "
                    f"Last count: {person_count} at {time_str}"
                )
                html_parts.append(
                    f'<li><strong>{camera}:</strong> {count} event(s) - '
                    f'Last count: {person_count} at {time_str}</li>'
                )
            
            html_parts.append('</ul>')
            body_parts.append("")
        
        body_parts.extend([
            "---",
            "InsightEye Surveillance System",
            "Login to view full details and footage."
        ])
        
        html_parts.extend([
            '<hr>',
            '<p style="color: #6c757d; font-size: 12px;">',
            'InsightEye Surveillance System<br>',
            'Login to view full details and footage.',
            '</p>',
            '</body></html>'
        ])
        
        body = "\n".join(body_parts)
        html_body = "".join(html_parts)
        
        # Send digest
        success = await send_email(
            user_email, 
            subject, 
            body, 
            html_body=html_body,
            skip_rate_limit=True
        )
        
        if success:
            logger.info(
                f"Digest email sent to {user_email} with {len(alerts)} alerts "
                f"({len(fire_alerts)} fire, {len(count_alerts)} count)"
            )
        else:
            logger.error(f"Failed to send digest email to {user_email}")


# Global digest instance
alert_digest = AlertDigest(digest_interval_minutes=15)


async def add_fire_alert_to_digest(
    user_email: str,
    camera_name: str,
    fire_status: str,
    location_info: Dict[str, Any] = None
):
    """Add fire alert to digest instead of sending immediately."""
    await alert_digest.add_alert(
        user_email,
        'fire',
        camera_name,
        {
            'fire_status': fire_status,
            'location_info': location_info
        }
    )


async def add_people_count_alert_to_digest(
    user_email: str,
    camera_name: str,
    person_count: int,
    threshold_settings: Dict[str, Any],
    location_info: Dict[str, Any] = None
):
    """Add people count alert to digest instead of sending immediately."""
    await alert_digest.add_alert(
        user_email,
        'people_count',
        camera_name,
        {
            'person_count': person_count,
            'threshold_settings': threshold_settings,
            'location_info': location_info
        }
    )