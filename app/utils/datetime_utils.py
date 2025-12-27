# app/utils/datetime_utils.py
"""Utilities for datetime conversion and handling."""

from zoneinfo import ZoneInfo
from datetime import datetime, timezone
from typing import Union


def datetime_to_iso(dt: datetime) -> str:
    """Convert datetime to ISO format string."""
    return dt.isoformat()


def iso_to_datetime(iso_str: str) -> datetime:
    """Convert ISO format string to datetime with timezone handling."""
    return datetime.fromisoformat(iso_str.replace('Z', '+00:00'))


def timestamp_to_iso(timestamp: Union[int, float]) -> str:
    """Convert Unix timestamp to ISO format string (UTC)."""
    return datetime.fromtimestamp(timestamp, tz=ZoneInfo("Africa/Cairo")).isoformat()


def iso_to_timestamp(iso_str: str) -> float:
    """Convert ISO format string to Unix timestamp (float for precision)."""
    return iso_to_datetime(iso_str).timestamp()
