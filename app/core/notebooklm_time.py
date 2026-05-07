"""Shared timestamp helpers for lightweight NotebookLM context/state."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone, tzinfo
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

_TIMEZONE_NAME = "Europe/Moscow"


def notebooklm_timezone() -> tzinfo:
    try:
        return ZoneInfo(_TIMEZONE_NAME)
    except ZoneInfoNotFoundError:
        return timezone(timedelta(hours=3))


def notebooklm_timezone_name() -> str:
    return _TIMEZONE_NAME


def to_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def to_notebooklm_local(value: datetime) -> datetime:
    return to_utc(value).astimezone(notebooklm_timezone())


def notebooklm_isoformat(value: datetime) -> str:
    return to_notebooklm_local(value).isoformat()


def parse_timestamp(value: str) -> datetime:
    return to_utc(datetime.fromisoformat(value))
