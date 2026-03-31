from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.dohod.ru"
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; heroincome-data/1.0; +https://github.com/amchercashin/heroincome-data)"}


# ---------------------------------------------------------------------------
# Pure parsing functions (no network I/O — easy to test)
# ---------------------------------------------------------------------------


def parse_tickers_from_index(html: str) -> list[str]:
    """Extract unique uppercase ticker symbols from the dohod.ru dividend index page."""
    soup = BeautifulSoup(html, "html.parser")
    pattern = re.compile(r"^/ik/analytics/dividend/([a-zA-Z0-9]+)$")
    seen: set[str] = set()
    tickers: list[str] = []
    for a in soup.find_all("a", href=pattern):
        match = pattern.match(a["href"])
        if match:
            ticker = match.group(1).upper()
            if ticker not in seen:
                seen.add(ticker)
                tickers.append(ticker)
    return sorted(tickers)


def _clean_cell(cell) -> str:
    """Extract plain text from a <td>, stripping tags and whitespace."""
    return cell.get_text(separator=" ", strip=True).replace("(прогноз)", "").strip()


def _parse_date(text: str) -> str | None:
    """Parse DD.MM.YYYY → YYYY-MM-DD, return None for blank/n/a."""
    text = text.strip()
    if not text or text.lower() == "n/a":
        return None
    try:
        return datetime.strptime(text, "%d.%m.%Y").strftime("%Y-%m-%d")
    except ValueError:
        return None


def _parse_amount(text: str) -> float | None:
    """Parse dividend amount string → float, return None for blank/n/a."""
    text = text.strip().replace(",", ".")
    if not text or text.lower() == "n/a":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_year(text: str) -> int | None:
    """Parse year string → int, return None for blank/n/a."""
    text = text.strip()
    if not text or text.lower() == "n/a":
        return None
    try:
        return int(text)
    except ValueError:
        return None


def parse_dividend_page(html: str, ticker: str) -> dict:
    """
    Parse a dohod.ru dividend page and return structured data.

    Returns:
        {
            "ticker": "LKOH",
            "scrapedAt": "2026-03-15T09:00:00Z",
            "source": "dohod.ru",
            "payments": [
                {
                    "recordDate": "2026-01-12",    # YYYY-MM-DD
                    "declaredDate": "2025-11-21",  # YYYY-MM-DD or null
                    "amount": 397.0,               # float or null
                    "year": 2025,                  # int or null
                    "isForecast": False
                },
                ...
            ]
        }
    """
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table", class_="content-table")
    if len(tables) < 3:
        raise ValueError(f"Expected ≥3 content-table tables on {ticker} page, found {len(tables)}")

    history_table = tables[2]
    payments: list[dict] = []

    for row in history_table.find_all("tr")[1:]:  # skip header row
        cells = row.find_all("td")
        if len(cells) < 4:
            continue

        is_forecast = "forecast" in row.get("class", [])
        declared_date = _parse_date(_clean_cell(cells[0]))
        record_date = _parse_date(_clean_cell(cells[1]))
        year = _parse_year(_clean_cell(cells[2]))
        amount = _parse_amount(_clean_cell(cells[3]))

        if record_date is None:
            continue  # строка без даты отсечки бесполезна

        payments.append({
            "recordDate": record_date,
            "declaredDate": declared_date,
            "amount": amount,
            "year": year,
            "isForecast": is_forecast,
        })

    return {
        "ticker": ticker.upper(),
        "scrapedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": "dohod.ru",
        "payments": payments,
    }
