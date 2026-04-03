from __future__ import annotations

import os
import time

from shared.network import create_session, fetch_with_retry
from shared.io import save_json, update_index
from stocks.dohod import BASE_URL as DOHOD_BASE, parse_tickers_from_index, parse_dividend_page
from stocks.smartlab import (
    SMARTLAB_BASE,
    parse_smartlab_tickers,
    parse_smartlab_dividend_page,
)

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "stocks")


def _scrape_dohod(session) -> None:
    """Scrape dohod.ru and save raw data to data/stocks/dohod/."""
    print("=== dohod.ru ===")
    resp = fetch_with_retry(session, f"{DOHOD_BASE}/ik/analytics/dividend/")
    if not resp:
        print("  ERROR: Could not fetch index page. Skipping dohod.ru.")
        return

    tickers = parse_tickers_from_index(resp.text)
    print(f"  Found {len(tickers)} tickers")

    successful = []
    for i, ticker in enumerate(tickers, 1):
        print(f"  [{i}/{len(tickers)}] {ticker}...")
        url = f"{DOHOD_BASE}/ik/analytics/dividend/{ticker.lower()}"
        resp = fetch_with_retry(session, url)
        if resp is None:
            print(f"    Skipping (not found or error)")
            continue
        try:
            data = parse_dividend_page(resp.text, ticker)
            path = os.path.join(DATA_DIR, "dohod", f"{ticker}.json")
            save_json(path, data)
            successful.append(ticker)
            print(f"    OK — {len(data['payments'])} payments")
        except (ValueError, KeyError) as exc:
            print(f"    Parse error: {exc}")
        time.sleep(1.5)

    update_index(os.path.join(DATA_DIR, "dohod", "index.json"), successful)
    print(f"  Done: {len(successful)}/{len(tickers)} tickers")


def _scrape_smartlab(session) -> None:
    """Scrape smartlab.ru and save raw data to data/stocks/smartlab/."""
    print("=== smartlab.ru ===")
    resp = fetch_with_retry(session, f"{SMARTLAB_BASE}/dividends/")
    if not resp:
        print("  ERROR: Could not fetch index page. Skipping smartlab.ru.")
        return

    tickers = parse_smartlab_tickers(resp.text)
    print(f"  Found {len(tickers)} tickers")

    successful = []
    for i, ticker in enumerate(tickers, 1):
        print(f"  [{i}/{len(tickers)}] {ticker}...")
        url = f"{SMARTLAB_BASE}/q/{ticker}/dividend/"
        resp = fetch_with_retry(session, url)
        if resp is None:
            print(f"    Skipping (not found or error)")
            continue
        try:
            data = parse_smartlab_dividend_page(resp.text, ticker)
            path = os.path.join(DATA_DIR, "smartlab", f"{ticker}.json")
            save_json(path, data)
            successful.append(ticker)
            print(f"    OK — {len(data['payments'])} payments")
        except (ValueError, KeyError) as exc:
            print(f"    Parse error: {exc}")
        time.sleep(1.0)

    update_index(os.path.join(DATA_DIR, "smartlab", "index.json"), successful)
    print(f"  Done: {len(successful)}/{len(tickers)} tickers")


def main() -> None:
    session = create_session()
    _scrape_dohod(session)
    _scrape_smartlab(session)
    print("All done.")


if __name__ == "__main__":
    main()
