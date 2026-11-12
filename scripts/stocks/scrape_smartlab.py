"""Scrape smart-lab.ru into data/stocks/smartlab/: one file per ticker plus index.json.

Run from scripts/: python -m stocks.scrape_smartlab
"""
from __future__ import annotations

import os
import sys
import time
from collections.abc import Callable
from datetime import datetime, timezone

from shared.io import save_json, update_index
from shared.network import create_session, fetch_with_retry
from stocks.smartlab import (
    CALENDAR_URL,
    SHARES_URL,
    page_url,
    parse_calendar,
    parse_dividend_page,
    parse_share_pages,
)

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "stocks", "smartlab")
PAUSE_SECONDS = 1.5
MAX_FAILURES_IN_ROW = 8


class ScrapeAborted(RuntimeError):
    """The site stopped answering (or blocked us): nothing more is written this run."""


def scrape(
    fetch: Callable[[str], str | None],
    data_dir: str,
    pause: float = PAUSE_SECONDS,
    max_failures_in_row: int = MAX_FAILURES_IN_ROW,
) -> None:
    print("=== smart-lab.ru ===")
    shares = fetch(SHARES_URL)
    if not shares:
        raise ScrapeAborted("shares list unavailable")
    pages = parse_share_pages(shares)
    codes = list(pages)
    listed = {ticker for tickers in pages.values() for ticker in tickers}
    if not codes:
        raise ScrapeAborted("no shares found on the shares list — layout changed?")

    # Without the calendar every upcoming row would lose its status, so the run stops here
    # rather than overwrite good files with worse ones.
    calendar_html = fetch(CALENDAR_URL)
    if not calendar_html:
        raise ScrapeAborted("dividend calendar unavailable")
    try:
        calendar = parse_calendar(calendar_html)
    except ValueError as exc:
        raise ScrapeAborted(f"dividend calendar: {exc}") from exc
    print(f"  {len(codes)} issuer pages, {len(calendar)} calendar entries")

    scraped_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    failures_in_row = 0
    pages_ok = 0
    written = 0
    for i, code in enumerate(codes, 1):
        if i > 1 and pause:
            time.sleep(pause)
        html = fetch(page_url(code))
        if html is None:
            failures_in_row += 1
            print(f"  [{i}/{len(codes)}] {code}: fetch failed")
            if failures_in_row >= max_failures_in_row:
                raise ScrapeAborted(f"{failures_in_row} pages in a row failed — blocked or down")
            continue
        failures_in_row = 0
        try:
            rows = parse_dividend_page(html, calendar, pages[code], listed)
        except ValueError as exc:
            print(f"  [{i}/{len(codes)}] {code}: parse error — {exc}")
            continue
        pages_ok += 1
        for ticker, payments in sorted(rows.items()):
            save_json(
                os.path.join(data_dir, f"{ticker}.json"),
                {"ticker": ticker, "page": page_url(code), "scrapedAt": scraped_at, "payments": payments},
            )
            written += 1
        print(f"  [{i}/{len(codes)}] {code}: " + (", ".join(f"{t} {len(p)}" for t, p in sorted(rows.items())) or "no rows"))

    if pages_ok == 0:
        raise ScrapeAborted("no page could be parsed — layout changed?")

    # The index lists every file on disk: a ticker whose page failed this run keeps its last data.
    os.makedirs(data_dir, exist_ok=True)
    tickers = sorted(f[:-5] for f in os.listdir(data_dir) if f.endswith(".json") and f != "index.json")
    update_index(os.path.join(data_dir, "index.json"), tickers)
    print(f"  Done: {pages_ok}/{len(codes)} pages, {written} tickers written, index has {len(tickers)}")


def main() -> None:
    session = create_session()

    def fetch(url: str) -> str | None:
        resp = fetch_with_retry(session, url)
        # smart-lab declares no charset in the header; requests would guess Latin-1.
        return resp.content.decode("utf-8", "replace") if resp is not None else None

    try:
        scrape(fetch, DATA_DIR)
    except ScrapeAborted as exc:
        print(f"  ABORTED: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
