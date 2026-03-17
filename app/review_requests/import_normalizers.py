from __future__ import annotations

import re
import unicodedata
from datetime import date, datetime, time, timezone, timedelta
from typing import Any, Optional
from zoneinfo import ZoneInfo


def normalize_name(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    s = str(value).strip().lower()
    if not s:
        return None

    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[,\.;:_\-]+", " ", s)
    s = re.sub(r"\b(sr|sra|dr|dra)\b\.?", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s or None


def choose_display_name(current: Optional[str], new_value: Optional[str]) -> Optional[str]:
    current = (current or "").strip() or None
    new_value = (new_value or "").strip() or None

    if not current:
        return new_value
    if not new_value:
        return current

    return new_value if len(new_value) > len(current) else current


def normalize_phone(value: Optional[str], default_country: str = "ES") -> Optional[str]:
    if not value:
        return None

    s = str(value).strip()
    if not s:
        return None

    s = re.sub(r"[^\d+]", "", s)
    if s.startswith("+"):
        digits = re.sub(r"\D", "", s[1:])
        if 8 <= len(digits) <= 15:
            return "+" + digits
        return None

    digits = re.sub(r"\D", "", s)
    if not digits:
        return None

    if default_country == "ES" and len(digits) == 9:
        return "+34" + digits

    if 8 <= len(digits) <= 15:
        return "+" + digits

    return None


def _parse_date(value: Any) -> Optional[date]:
    if value is None or value == "":
        return None

    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    s = str(value).strip()
    if not s:
        return None

    for fmt in (
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%d/%m/%y",
        "%d-%m-%y",
        "%Y/%m/%d",
    ):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            continue

    try:
        return datetime.fromisoformat(s.replace("Z", "")).date()
    except Exception:
        return None


def _parse_time(value: Any) -> Optional[time]:
    if value is None or value == "":
        return None

    if isinstance(value, datetime):
        return value.time().replace(second=0, microsecond=0)

    if isinstance(value, time):
        return value.replace(second=0, microsecond=0)

    s = str(value).strip()
    if not s:
        return None

    s = s.replace(".", ":")
    for fmt in ("%H:%M", "%H:%M:%S", "%H%M"):
        try:
            return datetime.strptime(s, fmt).time().replace(second=0, microsecond=0)
        except Exception:
            continue

    m = re.match(r"^(\d{1,2}):(\d{2})$", s)
    if m:
        hh = int(m.group(1))
        mm = int(m.group(2))
        if 0 <= hh <= 23 and 0 <= mm <= 59:
            return time(hour=hh, minute=mm)

    return None


def normalize_date_str(value: Any) -> Optional[str]:
    d = _parse_date(value)
    return d.isoformat() if d else None


def normalize_time_str(value: Any) -> Optional[str]:
    t = _parse_time(value)
    return t.strftime("%H:%M") if t else None


def build_appointment_at(
    date_str: Optional[str],
    time_str: Optional[str],
    timezone_str: Optional[str] = "Europe/Madrid",
) -> Optional[datetime]:
    if not date_str or not time_str:
        return None

    d = _parse_date(date_str)
    t = _parse_time(time_str)
    if not d or not t:
        return None

    tz = ZoneInfo(timezone_str or "Europe/Madrid")
    local_dt = datetime.combine(d, t).replace(tzinfo=tz)
    return local_dt.astimezone(timezone.utc)


def detect_missing_fields(
    name: Optional[str],
    phone: Optional[str],
    date_str: Optional[str],
    time_str: Optional[str],
) -> list[str]:
    missing: list[str] = []
    if not name:
        missing.append("name")
    if not phone:
        missing.append("phone")
    if not date_str:
        missing.append("date")
    if not time_str:
        missing.append("time")
    return missing


def is_older_than_24h(appointment_at: Optional[datetime], now: Optional[datetime] = None) -> bool:
    if not appointment_at:
        return False
    now = now or datetime.now(timezone.utc)
    if appointment_at.tzinfo is None:
        appointment_at = appointment_at.replace(tzinfo=timezone.utc)
    return appointment_at < (now - timedelta(hours=24))