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
from stocks.merge import merge_payments

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "stocks")


def _fetch_dohod(session, tickers: list[str]) -> dict[str, dict]:
    """Fetch dividend data from dohod.ru for given tickers."""
    results = {}
    for i, ticker in enumerate(tickers, 1):
        url = f"{DOHOD_BASE}/ik/analytics/dividend/{ticker.lower()}"
        resp = fetch_with_retry(session, url)
        if resp is None:
            continue
        try:
            results[ticker] = parse_dividend_page(resp.text, ticker)
        except (ValueError, KeyError) as exc:
            print(f"  dohod parse error for {ticker}: {exc}")
        time.sleep(1.5)
    return results


def _fetch_smartlab(session, tickers: list[str]) -> dict[str, dict]:
    """Fetch dividend data from smartlab.ru for given tickers."""
    results = {}
    for i, ticker in enumerate(tickers, 1):
        url = f"{SMARTLAB_BASE}/q/{ticker}/dividend/"
        resp = fetch_with_retry(session, url)
        if resp is None:
            continue
        try:
            results[ticker] = parse_smartlab_dividend_page(resp.text, ticker)
        except (ValueError, KeyError) as exc:
            print(f"  smartlab parse error for {ticker}: {exc}")
        time.sleep(1.0)
    return results


def main() -> None:
    session = create_session()

    # Step 1: Discover tickers from both sources
    print("Step 1: Discovering tickers...")

    dohod_tickers = []
    resp = fetch_with_retry(session, f"{DOHOD_BASE}/ik/analytics/dividend/")
    if resp:
        dohod_tickers = parse_tickers_from_index(resp.text)
        print(f"  dohod.ru: {len(dohod_tickers)} tickers")
    else:
        print("  WARNING: dohod.ru index unavailable")

    smartlab_tickers = []
    resp = fetch_with_retry(session, f"{SMARTLAB_BASE}/dividends/")
    if resp:
        smartlab_tickers = parse_smartlab_tickers(resp.text)
        print(f"  smartlab.ru: {len(smartlab_tickers)} tickers")
    else:
        print("  WARNING: smartlab.ru index unavailable")

    all_tickers = sorted(set(dohod_tickers) | set(smartlab_tickers))
    print(f"  Combined: {len(all_tickers)} unique tickers")

    # Step 2: Fetch from both sources
    print("Step 2: Fetching from dohod.ru...")
    dohod_data = _fetch_dohod(session, dohod_tickers)
    print(f"  Got {len(dohod_data)} tickers from dohod.ru")

    print("Step 3: Fetching from smartlab.ru...")
    smartlab_data = _fetch_smartlab(session, all_tickers)
    print(f"  Got {len(smartlab_data)} tickers from smartlab.ru")

    # Step 4: Merge and save
    print("Step 4: Merging and saving...")
    successful = []
    for ticker in all_tickers:
        d = dohod_data.get(ticker)
        s = smartlab_data.get(ticker)
        if not d and not s:
            continue

        payments = merge_payments(d, s, ticker)
        scraped_at = (d or s)["scrapedAt"]

        result = {
            "ticker": ticker,
            "isin": None,  # populated if MOEX ISS lookup is added
            "scrapedAt": scraped_at,
            "payments": payments,
        }

        path = os.path.join(DATA_DIR, "dividends", f"{ticker}.json")
        save_json(path, result)
        successful.append(ticker)

    print(f"Step 5: Updating index ({len(successful)}/{len(all_tickers)} tickers)...")
    update_index(os.path.join(DATA_DIR, "index.json"), successful)
    print("Done.")


if __name__ == "__main__":
    main()
