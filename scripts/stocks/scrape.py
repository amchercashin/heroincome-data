from __future__ import annotations

import os
import time

from shared.network import create_session, fetch_with_retry
from shared.io import save_json, update_index
from stocks.dohod import BASE_URL, parse_tickers_from_index, parse_dividend_page

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "stocks")


def main() -> None:
    session = create_session()

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
            path = os.path.join(DATA_DIR, "dividends", f"{data['ticker']}.json")
            save_json(path, data)
            successful.append(ticker)
            print(f"  OK — {len(data['payments'])} payments")
        except (ValueError, KeyError) as exc:
            print(f"  Parse error for {ticker}: {exc}")
        time.sleep(1.5)

    print(f"Step 3: Updating index ({len(successful)}/{len(tickers)} tickers)...")
    update_index(os.path.join(DATA_DIR, "index.json"), successful)
    print("Done.")


if __name__ == "__main__":
    main()
