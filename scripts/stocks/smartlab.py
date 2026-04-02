from __future__ import annotations

import json
import re
from datetime import datetime, timezone

from bs4 import BeautifulSoup

from shared.dates import parse_date_dmy

SMARTLAB_BASE = "https://smart-lab.ru"


def parse_smartlab_tickers(html: str) -> list[str]:
    """Extract tickers from aBubbleData JS variable on /dividends/ page."""
    match = re.search(r"var\s+aBubbleData\s*=\s*(\[.*?\]);", html, re.DOTALL)
    if not match:
        return []
    try:
        data = json.loads(match.group(1))
        tickers = sorted({item["secid"].upper() for item in data if "secid" in item})
        return tickers
    except (json.JSONDecodeError, KeyError):
        return []


def _parse_amount(text: str) -> float | None:
    text = text.strip().replace(",", ".").replace("\xa0", "").replace(" ", "")
    if not text or text.lower() == "n/a" or text == "-":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def parse_smartlab_dividend_page(html: str, ticker: str) -> dict:
    """Parse /q/{TICKER}/dividend/ page.

    Returns payments with status: 'paid', 'approved', or 'forecast'.
    - Table 0 = expected dividends (approved if has CSS class, forecast otherwise)
    - Table 1 = paid dividends (always 'paid')
    """
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    payments: list[dict] = []

    for table_idx, table in enumerate(tables):
        for row in table.find_all("tr")[1:]:  # skip header
            cells = row.find_all("td")
            if len(cells) < 5:
                continue

            record_date = parse_date_dmy(cells[2].get_text(strip=True))
            if not record_date:
                continue

            amount = _parse_amount(cells[4].get_text(strip=True))

            if table_idx == 0:
                has_approved_class = "dividend_approved" in row.get("class", [])
                status = "approved" if has_approved_class else "forecast"
            else:
                status = "paid"

            year_text = cells[3].get_text(strip=True)
            year = None
            year_match = re.search(r"(\d{4})", year_text)
            if year_match:
                year = int(year_match.group(1))

            payments.append(
                {
                    "recordDate": record_date,
                    "amount": amount,
                    "year": year,
                    "status": status,
                }
            )

    return {
        "ticker": ticker.upper(),
        "scrapedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "payments": payments,
    }
