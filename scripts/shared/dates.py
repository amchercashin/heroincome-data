from __future__ import annotations

from datetime import datetime


def parse_date_dmy(text: str) -> str | None:
    """Parse DD.MM.YYYY -> YYYY-MM-DD. Returns None for blank/n/a."""
    text = text.strip()
    if not text or text.lower() == "n/a":
        return None
    try:
        return datetime.strptime(text, "%d.%m.%Y").strftime("%Y-%m-%d")
    except ValueError:
        return None


def parse_date_iso(text: str) -> str | None:
    """Validate and normalize YYYY-MM-DD. Returns None for invalid."""
    text = text.strip()
    if not text or text.lower() == "n/a":
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        return None
