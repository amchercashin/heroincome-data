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
    if len(tables) < 2:
        raise ValueError(f"Expected ≥2 content-table tables on {ticker} page, found {len(tables)}")

    history_table = tables[1]
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


# ---------------------------------------------------------------------------
# Network layer
# ---------------------------------------------------------------------------


def fetch_with_retry(session: requests.Session, url: str, retries: int = 3) -> requests.Response | None:
    """Fetch URL with exponential backoff. Returns None on 404 or after exhausting retries."""
    delays = [1, 4, 16]
    for attempt in range(retries):
        try:
            response = session.get(url, headers=HEADERS, timeout=30)
            if response.status_code == 404:
                return None
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            if attempt < retries - 1:
                wait = delays[attempt]
                print(f"  Attempt {attempt + 1} failed ({exc}), retrying in {wait}s...")
                time.sleep(wait)
            else:
                print(f"  Failed after {retries} attempts: {url} — {exc}")
                return None
    return None


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------


def save_ticker_data(data: dict) -> None:
    dividends_dir = os.path.join(DATA_DIR, "dividends")
    os.makedirs(dividends_dir, exist_ok=True)
    path = os.path.join(dividends_dir, f"{data['ticker']}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def update_index(tickers: list[str]) -> None:
    index = {
        "updatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "tickerCount": len(tickers),
        "tickers": sorted(tickers),
    }
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(os.path.join(DATA_DIR, "index.json"), "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    session = requests.Session()

    print("Step 1: Discovering tickers from dohod.ru index...")
    response = fetch_with_retry(session, f"{BASE_URL}/ik/analytics/dividend/")
    if not response:
        print("ERROR: Could not fetch index page. Aborting.")
        return
    tickers = parse_tickers_from_index(response.text)
    print(f"  Found {len(tickers)} tickers: {', '.join(tickers[:5])}...")

    print("Step 2: Scraping each ticker...")
    successful: list[str] = []
    for i, ticker in enumerate(tickers, 1):
        print(f"  [{i}/{len(tickers)}] {ticker}...")
        url = f"{BASE_URL}/ik/analytics/dividend/{ticker.lower()}"
        resp = fetch_with_retry(session, url)
        if resp is None:
            print(f"  Skipping {ticker} (not found or error)")
            continue
        try:
            data = parse_dividend_page(resp.text, ticker)
            save_ticker_data(data)
            successful.append(ticker)
            payment_count = len(data["payments"])
            print(f"  OK — {payment_count} payments")
        except (ValueError, KeyError) as exc:
            print(f"  Parse error for {ticker}: {exc}")
        time.sleep(1.5)

    print(f"Step 3: Updating index ({len(successful)}/{len(tickers)} tickers)...")
    update_index(successful)
    print("Done.")


if __name__ == "__main__":
    main()
