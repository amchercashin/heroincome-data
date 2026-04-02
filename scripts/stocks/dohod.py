from __future__ import annotations

import re
from datetime import datetime, timezone

from bs4 import BeautifulSoup

from shared.dates import parse_date_dmy

BASE_URL = "https://www.dohod.ru"


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
    return cell.get_text(separator=" ", strip=True).replace("(прогноз)", "").strip()


def _parse_amount(text: str) -> float | None:
    text = text.strip().replace(",", ".")
    if not text or text.lower() == "n/a":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_year(text: str) -> int | None:
    text = text.strip()
    if not text or text.lower() == "n/a":
        return None
    try:
        return int(text)
    except ValueError:
        return None


def parse_dividend_page(html: str, ticker: str) -> dict:
    """Parse a dohod.ru dividend page and return structured data."""
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table", class_="content-table")
    if len(tables) < 2:
        raise ValueError(
            f"Expected >=2 content-table tables on {ticker} page, found {len(tables)}"
        )

    history_table = tables[1]
    payments: list[dict] = []

    for row in history_table.find_all("tr")[1:]:
        cells = row.find_all("td")
        if len(cells) < 4:
            continue

        is_forecast = "forecast" in row.get("class", [])
        declared_date = parse_date_dmy(_clean_cell(cells[0]))
        record_date = parse_date_dmy(_clean_cell(cells[1]))
        year = _parse_year(_clean_cell(cells[2]))
        amount = _parse_amount(_clean_cell(cells[3]))

        if record_date is None:
            continue

        payments.append(
            {
                "recordDate": record_date,
                "declaredDate": declared_date,
                "amount": amount,
                "year": year,
                "isForecast": is_forecast,
            }
        )

    return {
        "ticker": ticker.upper(),
        "scrapedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "payments": payments,
    }
