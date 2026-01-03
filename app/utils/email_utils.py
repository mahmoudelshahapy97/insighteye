# app/utils/email_utils.py
"""Email utilities for sending notifications and alerts with rate limiting."""

import logging
import asyncio
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from zoneinfo import ZoneInfo
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any
from collections import defaultdict

from app.config.settings import config

logger = logging.getLogger(__name__)

# Rate limiting tracking
_last_email_sent = defaultdict(lambda: None)
_email_rate_limits = {
    'fire_alert': timedelta(minutes=10),  # 10 minutes between fire alerts per camera
    'people_count': timedelta(minutes=5),  # 5 minutes between count alerts per camera
    'general': timedelta(seconds=30)  # 30 seconds between general emails
}

_daily_email_count = 0
_daily_count_reset = datetime.now(ZoneInfo("Africa/Cairo")).date()

def _check_daily_limit():
    global _daily_email_count, _daily_count_reset
    
    today = datetime.now(ZoneInfo("Africa/Cairo")).date()
    if today > _daily_count_reset:
        _daily_email_count = 0
        _daily_count_reset = today
    
    max_daily = 100  # Set conservative limit
    if _daily_email_count >= max_daily:
        logger.warning(f"Daily email limit reached: {_daily_email_count}/{max_daily}")
        return False
    
    _daily_email_count += 1
    return True
    
def _can_send_email(alert_type: str, identifier: str) -> bool:
    """
    Check if enough time has passed since last email of this type.
    
    Args:
        alert_type: Type of alert ('fire_alert', 'people_count', 'general')
        identifier: Unique identifier (e.g., camera_name or user_email)
    
    Returns:
        bool: True if email can be sent, False otherwise
    """
    key = f"{alert_type}:{identifier}"
    last_sent = _last_email_sent.get(key)
    
    if last_sent is None:
        return True
    
    rate_limit = _email_rate_limits.get(alert_type, _email_rate_limits['general'])
    time_since_last = datetime.now(ZoneInfo("Africa/Cairo")) - last_sent
    
    if time_since_last < rate_limit:
        remaining = (rate_limit - time_since_last).total_seconds()
        logger.warning(
            f"Rate limit: Email type '{alert_type}' for '{identifier}' "
            f"blocked. {remaining:.0f}s remaining until next allowed send."
        )
        return False
    
    return True


def _mark_email_sent(alert_type: str, identifier: str):
    """Mark that an email of this type was sent."""
    key = f"{alert_type}:{identifier}"
    _last_email_sent[key] = datetime.now(ZoneInfo("Africa/Cairo"))


async def send_email(
    recipient_email: str, 
    subject: str, 
    body: str, 
    html_body: Optional[str] = None,
    skip_rate_limit: bool = False
) -> bool:
    """Send email asynchronously with optional rate limiting."""
    # Rate limiting for general emails
    if not skip_rate_limit:
        if not _can_send_email('general', recipient_email):
            return False
    
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        None, 
        _send_email_sync, 
        recipient_email, 
        subject, 
        body, 
        html_body
    )
    
    if result and not skip_rate_limit:
        _mark_email_sent('general', recipient_email)
    
    return result


def _send_email_sync(
    recipient_email: str, 
    subject: str, 
    body: str, 
    html_body: Optional[str] = None
) -> bool:
    """Synchronous helper for sending email."""
    app_sender_email = config.sender_email
    app_sender_password = config.sender_password
    smtp_server_host = config.smtp_server
    smtp_server_port = int(config.smtp_port)

    if not all([app_sender_email, app_sender_password, smtp_server_host, smtp_server_port]):
        logger.error("SMTP server, port, or credentials not fully configured for send_email.")
        return False

    message = MIMEMultipart("alternative")
    message["Subject"] = subject
    message["From"] = app_sender_email
    message["To"] = recipient_email
    
    message.attach(MIMEText(body, "plain", _charset="utf-8"))
    if html_body:
        message.attach(MIMEText(html_body, "html", _charset="utf-8"))

    context = ssl.create_default_context()
    try:
        with smtplib.SMTP(smtp_server_host, smtp_server_port, timeout=config.smtp_timeout) as server:
            server.ehlo_or_helo_if_needed()
            if server.has_extn('STARTTLS'):
                server.starttls(context=context)
                server.ehlo_or_helo_if_needed()
            server.login(app_sender_email, app_sender_password)
            server.sendmail(app_sender_email, recipient_email, message.as_string())
        logger.info(f"Email sent successfully to {recipient_email} with subject '{subject}'")
        return True
    except smtplib.SMTPDataError as e:
        # Specifically handle daily limit errors
        if '5.4.5' in str(e) or 'Daily user sending limit exceeded' in str(e):
            logger.critical(
                f"GMAIL DAILY SENDING LIMIT EXCEEDED! "
                f"Cannot send email to {recipient_email}. "
                f"Consider: 1) Switching to transactional email service (SendGrid, AWS SES), "
                f"2) Using Google Workspace account (2000 emails/day), "
                f"3) Implementing email batching/digest system"
            )
        else:
            logger.error(f"SMTP Data Error sending email to {recipient_email}: {e}", exc_info=True)
        return False
    except smtplib.SMTPAuthenticationError:
        logger.error(f"SMTP Authentication Error for {app_sender_email}. Check credentials.", exc_info=True)
        return False
    except smtplib.SMTPRecipientsRefused:
        logger.error(f"Recipient refused for email to {recipient_email}.", exc_info=True)
        return False
    except smtplib.SMTPException as e_smtp:
        logger.error(f"SMTP Error sending email to {recipient_email}: {e_smtp}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"General error in _send_email_sync for {recipient_email}: {e}", exc_info=True)
        return False


async def send_email_from_client_to_admin(
    admin_recipient_email: str, 
    subject: str, 
    body: str, 
    reply_to_email: Optional[str] = None
) -> bool:
    """Send notification email to admin from client."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        None, 
        _send_email_from_client_to_admin_sync, 
        admin_recipient_email, 
        subject, 
        body, 
        reply_to_email
    )


def _send_email_from_client_to_admin_sync(
    admin_recipient_email: str, 
    subject: str, 
    body: str, 
    reply_to_email: Optional[str] = None
) -> bool:
    """Synchronous helper for sending admin notification email."""
    app_sender_email = config.sender_email
    app_sender_password = config.sender_password
    smtp_server_host = config.smtp_server
    smtp_server_port = int(config.smtp_port)

    if not all([app_sender_email, app_sender_password, smtp_server_host, smtp_server_port]):
        logger.error("SMTP config incomplete for send_email_from_client_to_admin.")
        return False

    message = MIMEMultipart()
    message["From"] = f"InsightEye System <{app_sender_email}>" 
    message["To"] = admin_recipient_email
    message["Subject"] = subject
    if reply_to_email:
        message.add_header('Reply-To', reply_to_email)
    
    message.attach(MIMEText(body, "plain", _charset="utf-8"))

    context = ssl.create_default_context()
    try:
        with smtplib.SMTP(smtp_server_host, smtp_server_port, timeout=config.smtp_timeout) as server:
            server.ehlo_or_helo_if_needed()
            if server.has_extn('STARTTLS'):
                server.starttls(context=context)
                server.ehlo_or_helo_if_needed()
            server.login(app_sender_email, app_sender_password)
            server.sendmail(app_sender_email, admin_recipient_email, message.as_string())
        logger.info(f"Admin notification email sent to {admin_recipient_email} regarding '{subject}' from {reply_to_email or 'system'}")
        return True
    except smtplib.SMTPDataError as e:
        if '5.4.5' in str(e) or 'Daily user sending limit exceeded' in str(e):
            logger.critical("GMAIL DAILY SENDING LIMIT EXCEEDED for admin notification!")
        else:
            logger.error(f"SMTP Data Error for admin notification: {e}", exc_info=True)
        return False
    except smtplib.SMTPAuthenticationError:
        logger.error(f"SMTP Authentication Error for {app_sender_email} (admin notification). Check credentials.", exc_info=True)
        return False
    except smtplib.SMTPRecipientsRefused:
        logger.error(f"Recipient refused for admin notification email to {admin_recipient_email}.", exc_info=True)
        return False
    except smtplib.SMTPException as e_smtp:
        logger.error(f"SMTP Error sending admin notification to {admin_recipient_email}: {e_smtp}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"General error in _send_email_from_client_to_admin_sync for {admin_recipient_email}: {e}", exc_info=True)
        return False


async def send_fire_alert_email(
    user_email: str, 
    camera_name: str, 
    fire_status: str, 
    location_info: Optional[Dict[str, Any]] = None
) -> bool:
    """
    Send fire/smoke alert email to user with rate limiting.
    Will not send more than once per 10 minutes per camera.
    """
    # Check rate limit
    # ✅ FIX: Use camera_name as identifier, not user_email:camera_name
    # This allows multiple users to get alerts for same camera
    # identifier = f"{user_email}:{camera_name}"
    identifier = camera_name
    if not _can_send_email('fire_alert', identifier):
        logger.info(
            f"Fire alert email rate-limited for {camera_name}. "
            f"Skipping duplicate alert to prevent spam."
        )
        return False
    
    # ✅ FIX: Check daily limit
    if not _check_daily_limit():
        logger.error(f"Daily email limit exceeded, cannot send fire alert to {user_email}")
        return False
    
    try:
        logger.info(f"Preparing to send fire alert email to {user_email} for {fire_status} detected at {camera_name}")
        
        location_text = ""
        location_parts = []
        
        if location_info:
            if location_info.get('location'):
                location_parts.append(f"Location: {location_info['location']}")
            if location_info.get('building'):
                location_parts.append(f"Building: {location_info['building']}")
            if location_info.get('area'):
                location_parts.append(f"Area: {location_info['area']}")
            if location_info.get('zone'):
                location_parts.append(f"Zone: {location_info['zone']}")
            if location_info.get('floor_level'):
                location_parts.append(f"Floor: {location_info['floor_level']}")
            
            if location_parts:
                location_text = f"\nLocation Details:\n" + "\n".join(f"  • {part}" for part in location_parts)
        
        timestamp = datetime.now(ZoneInfo("Africa/Cairo")).strftime("%Y-%m-%d %H:%M:%S UTC")
        alert_type = "FIRE" if fire_status == "fire" else "SMOKE"
        subject = f"🚨 URGENT: {alert_type} DETECTED - {camera_name}"
        
        body = f"""URGENT ALERT: {alert_type} DETECTED

Camera: {camera_name}
Status: {fire_status.upper()}
Time: {timestamp}{location_text}

This is an automated alert from your InsightEye surveillance system. 
Please verify the situation immediately and take appropriate action.

If this is a false alarm, please check your camera positioning and detection settings.

---
InsightEye Surveillance System
This alert will not be sent again for the next 10 minutes to prevent spam.
"""

        html_body = f"""
<html>
<body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
    <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
        <div style="background-color: #dc3545; color: white; padding: 15px; border-radius: 5px; text-align: center; margin-bottom: 20px;">
            <h1 style="margin: 0; font-size: 24px;">🚨 URGENT ALERT</h1>
            <h2 style="margin: 5px 0 0 0; font-size: 20px;">{alert_type} DETECTED</h2>
        </div>
        
        <div style="background-color: #f8f9fa; padding: 20px; border-radius: 5px; margin-bottom: 20px;">
            <h3 style="color: #dc3545; margin-top: 0;">Alert Details:</h3>
            <ul style="list-style: none; padding: 0;">
                <li style="padding: 5px 0;"><strong>Camera:</strong> {camera_name}</li>
                <li style="padding: 5px 0;"><strong>Status:</strong> <span style="color: #dc3545; font-weight: bold;">{fire_status.upper()}</span></li>
                <li style="padding: 5px 0;"><strong>Time:</strong> {timestamp}</li>
            </ul>
            {f'<h4>Location Details:</h4><ul style="list-style: none; padding-left: 20px;">' + ''.join(f'<li style="padding: 2px 0;">• {part}</li>' for part in location_parts) + '</ul>' if location_parts else ''}
        </div>
        
        <div style="background-color: #fff3cd; border: 1px solid #ffeaa7; padding: 15px; border-radius: 5px; margin-bottom: 20px;">
            <h4 style="color: #856404; margin-top: 0;">⚠️ Immediate Action Required</h4>
            <p style="margin-bottom: 0; color: #856404;">Please verify the situation immediately and take appropriate safety measures.</p>
        </div>
        
        <div style="font-size: 12px; color: #6c757d; border-top: 1px solid #dee2e6; padding-top: 15px;">
            <p>This is an automated alert from your InsightEye surveillance system.</p>
            <p>If this is a false alarm, please check your camera positioning and detection settings.</p>
            <p><strong>Note:</strong> This alert will not be sent again for the next 10 minutes to prevent spam.</p>
        </div>
    </div>
</body>
</html>
"""

        success = await send_email(user_email, subject, body, html_body=html_body, skip_rate_limit=True)
        
        if success:
            _mark_email_sent('fire_alert', identifier)
            logger.info(f"✅ Fire alert email sent successfully to {user_email} for {fire_status} at {camera_name}")
        else:
            logger.error(f"❌ Failed to send fire alert email to {user_email} for {fire_status} at {camera_name}")
        
        return success
        
    except Exception as e:
        logger.error(f"Error sending fire alert email to {user_email}: {e}", exc_info=True)
        return False


async def send_people_count_alert_email(
    user_email: str, 
    camera_name: str, 
    person_count: int, 
    threshold_settings: Dict[str, Any],
    location_info: Optional[Dict[str, Any]] = None
) -> bool:
    """
    Send people count threshold alert email to user with rate limiting.
    Will not send more than once per 5 minutes per camera.
    """
    # Check rate limit
    # ✅ FIX: Use camera_name as identifier
    # identifier = f"{user_email}:{camera_name}"
    identifier = camera_name
    if not _can_send_email('people_count', identifier):
        logger.info(
            f"People count alert email rate-limited for {camera_name}. "
            f"Skipping duplicate alert to prevent spam."
        )
        return False

    # ✅ FIX: Check daily limit
    if not _check_daily_limit():
        logger.error(f"Daily email limit exceeded, cannot send people count alert to {user_email}")
        return False
    
    try:
        logger.info(f"Preparing to send people count alert email to {user_email} for {person_count} people at {camera_name}")
        
        location_text = ""
        location_parts = []
        
        if location_info:
            if location_info.get('location'):
                location_parts.append(f"Location: {location_info['location']}")
            if location_info.get('building'):
                location_parts.append(f"Building: {location_info['building']}")
            if location_info.get('area'):
                location_parts.append(f"Area: {location_info['area']}")
            if location_info.get('zone'):
                location_parts.append(f"Zone: {location_info['zone']}")
            if location_info.get('floor_level'):
                location_parts.append(f"Floor: {location_info['floor_level']}")
            
            if location_parts:
                location_text = f"\nLocation Details:\n" + "\n".join(f"  • {part}" for part in location_parts)
        
        timestamp = datetime.now(ZoneInfo("Africa/Cairo")).strftime("%Y-%m-%d %H:%M:%S UTC")
        
        greater_than = threshold_settings.get("greater_than")
        less_than = threshold_settings.get("less_than")
        
        if greater_than is not None and person_count > greater_than:
            alert_type = "HIGH OCCUPANCY"
            threshold_info = f"Detected: {person_count} people (Threshold: >{greater_than})"
            severity_color = "#dc3545"
        elif less_than is not None and person_count < less_than:
            alert_type = "LOW OCCUPANCY"
            threshold_info = f"Detected: {person_count} people (Threshold: <{less_than})"
            severity_color = "#fd7e14"
        else:
            alert_type = "OCCUPANCY"
            threshold_info = f"Detected: {person_count} people"
            severity_color = "#6f42c1"
        
        subject = f"🚨 {alert_type} ALERT - {camera_name}"
        
        body = f"""{alert_type} ALERT

Camera: {camera_name}
{threshold_info}
Time: {timestamp}{location_text}

This is an automated alert from your InsightEye surveillance system.
The people count has exceeded your configured threshold settings.

Please review the situation and adjust thresholds if necessary.

---
InsightEye Surveillance System
Configure your alert settings in the camera management section.
"""

        html_body = f"""
<html>
<body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
    <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
        <div style="background-color: {severity_color}; color: white; padding: 15px; border-radius: 5px; text-align: center; margin-bottom: 20px;">
            <h1 style="margin: 0; font-size: 24px;">🚨 OCCUPANCY ALERT</h1>
            <h2 style="margin: 5px 0 0 0; font-size: 20px;">{alert_type}</h2>
        </div>
        
        <div style="background-color: #f8f9fa; padding: 20px; border-radius: 5px; margin-bottom: 20px;">
            <h3 style="color: {severity_color}; margin-top: 0;">Alert Details:</h3>
            <ul style="list-style: none; padding: 0;">
                <li style="padding: 5px 0;"><strong>Camera:</strong> {camera_name}</li>
                <li style="padding: 5px 0;"><strong>Count:</strong> <span style="color: {severity_color}; font-weight: bold;">{person_count} people</span></li>
                <li style="padding: 5px 0;"><strong>Threshold:</strong> {f'>{greater_than}' if greater_than is not None and person_count > greater_than else f'<{less_than}' if less_than is not None and person_count < less_than else 'N/A'}</li>
                <li style="padding: 5px 0;"><strong>Time:</strong> {timestamp}</li>
            </ul>
            {f'<h4>Location Details:</h4><ul style="list-style: none; padding-left: 20px;">' + ''.join(f'<li style="padding: 2px 0;">• {part}</li>' for part in (location_parts if location_info else [])) + '</ul>' if location_info and location_parts else ''}
        </div>
        
        <div style="background-color: #e7f3ff; border: 1px solid #b3d9ff; padding: 15px; border-radius: 5px; margin-bottom: 20px;">
            <h4 style="color: #0056b3; margin-top: 0;">📊 Occupancy Monitoring</h4>
            <p style="margin-bottom: 0; color: #0056b3;">This alert indicates that the people count has triggered your configured threshold. Please review the situation and adjust settings if needed.</p>
        </div>
        
        <div style="font-size: 12px; color: #6c757d; border-top: 1px solid #dee2e6; padding-top: 15px;">
            <p>This is an automated alert from your InsightEye surveillance system.</p>
            <p>You can configure alert thresholds in the camera management section of your dashboard.</p>
        </div>
    </div>
</body>
</html>
"""

        success = await send_email(user_email, subject, body, html_body=html_body, skip_rate_limit=True)
        
        if success:
            _mark_email_sent('people_count', identifier)
            logger.info(f"✅ People count alert email sent successfully to {user_email} for {person_count} people at {camera_name}")
        else:
            logger.error(f"❌ Failed to send people count alert email to {user_email} for {person_count} people at {camera_name}")
        
        return success
        
    except Exception as e:
        logger.error(f"Error sending people count alert email to {user_email}: {e}", exc_info=True)
        return False