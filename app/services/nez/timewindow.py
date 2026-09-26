"""Zone operating-schedule evaluation (ported from features/no-entry-zone)."""

from __future__ import annotations

from datetime import datetime, time, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


def get_zoneinfo(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError):
        return ZoneInfo("UTC")


def to_local(moment: datetime, tz_name: str) -> datetime:
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(get_zoneinfo(tz_name))


def _parse_time(value: str) -> time:
    hour, minute = value.split(":")[:2]
    return time(int(hour), int(minute))


def is_zone_active(schedule: dict | None, now_local: datetime) -> bool:
    """Is the zone armed right now?

    ``schedule`` is ``{"days": [0..6], "start": "HH:MM", "end": "HH:MM"}`` with 0 = Monday.
    A window whose end is earlier than its start spans midnight (e.g. 18:00 -> 07:00);
    in that case ``days`` refers to the day the window *starts* on. Both bounds are
    inclusive to the minute, so "end": "23:59" covers the whole last minute of the day.
    """
    if not schedule:
        return True

    days = schedule.get("days") or list(range(7))
    start = _parse_time(schedule.get("start") or "00:00")
    end = _parse_time(schedule.get("end") or "23:59")
    weekday = now_local.weekday()
    current = now_local.time().replace(second=0, microsecond=0)

    if start <= end:
        return weekday in days and start <= current <= end

    # Overnight window: either late on a scheduled day, or early on the day after one.
    if weekday in days and current >= start:
        return True
    previous_day = (weekday - 1) % 7
    return previous_day in days and current <= end
