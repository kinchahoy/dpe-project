from __future__ import annotations

from datetime import datetime, timezone


def utc_now() -> datetime:
    """Return a naive datetime representing UTC 'now'.

    SQLite doesn't store timezone-aware datetimes well by default, but we still want
    to generate timestamps from a timezone-aware clock.
    """

    return datetime.now(timezone.utc).replace(tzinfo=None)
