from __future__ import annotations

import json
import os
from datetime import date

from shared.network import create_session, fetch_with_retry
from shared.io import save_json, update_index
from funds.parus import PARUS_FUNDS, SHEETS_CSV_URL, parse_parus_csv
from funds.parus_site import FUNDS_LIST_URL, discover_fund_urls, parse_fund_page, pick_secid

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "funds")
DIST_DIR = os.path.join(DATA_DIR, "distributions")
INDEX_PATH = os.path.join(DATA_DIR, "index.json")
MOEX_SEARCH_URL = (
    "https://iss.moex.com/iss/securities.json?q={isin}"
    "&iss.meta=off&iss.only=securities&securities.columns=secid,isin"
)


def _scrape_site(session, today: date) -> dict[str, dict]:
    """All funds with payouts on parus.properties, by ISIN."""
    print(f"=== parus.properties ===")
    listing = fetch_with_retry(session, FUNDS_LIST_URL)
    if not listing:
        print("  ERROR: fund list unavailable")
        return {}
    urls = discover_fund_urls(listing.text)
    print(f"  Found {len(urls)} fund pages")

    found: dict[str, dict] = {}
    for url in urls:
        resp = fetch_with_retry(session, url)
        if not resp:
            print(f"  {url}: fetch failed")
            continue
        try:
            data = parse_fund_page(resp.text, url, today)
        except ValueError as exc:
            print(f"  {url}: parse error — {exc}")
            continue
        if data is None:
            print(f"  {url}: no payouts table (skipped)")
            continue
        if not data["distributions"]:
            print(f"  {url}: payouts table is empty (skipped)")
            continue
        found[data["isin"]] = data
        print(f"  {data['name']} ({data['isin']}) — {len(data['distributions'])} distributions")
    return found


def _scrape_sheets(session, isins: set[str]) -> dict[str, dict]:
    """Fallback: Google Sheets of the known funds the site did not give."""
    found: dict[str, dict] = {}
    for fund in PARUS_FUNDS:
        if fund["isin"] not in isins:
            continue
        name = fund["name"]
        print(f"  {name}: falling back to Google Sheets...")
        resp = fetch_with_retry(session, SHEETS_CSV_URL.format(sheet_id=fund["sheetId"]))
        if not resp:
            print(f"  {name}: fetch failed")
            continue
        try:
            data = parse_parus_csv(resp.content.decode("utf-8"), name=name, isin=fund["isin"])
        except (ValueError, KeyError) as exc:
            print(f"  {name}: parse error — {exc}")
            continue
        if data["distributions"]:
            data["source"] = "google-sheets"
            found[fund["isin"]] = data
    return found


def _moex_secid(session, isin: str) -> str | None:
    resp = fetch_with_retry(session, MOEX_SEARCH_URL.format(isin=isin))
    if not resp:
        return None
    try:
        return pick_secid(resp.json()["securities"]["data"], isin)
    except (ValueError, KeyError, TypeError):
        return None


def _load_index() -> dict:
    try:
        with open(INDEX_PATH, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def main() -> None:
    session = create_session()
    today = date.today()

    funds = _scrape_site(session, today)
    missing = {f["isin"] for f in PARUS_FUNDS} - funds.keys()
    if missing:
        funds.update(_scrape_sheets(session, missing))

    # Files are keyed by ISIN. When the MOEX code differs (XTRIUMF), it becomes
    # an alias so an asset known only by its ticker still finds its file.
    aliases: dict[str, str] = dict(_load_index().get("aliases", {}))
    for isin, data in sorted(funds.items()):
        secid = _moex_secid(session, isin)
        if secid and secid != isin:
            aliases[secid] = isin
        data["ticker"] = secid if secid and secid != isin else None
        save_json(os.path.join(DIST_DIR, f"{isin}.json"), data)

    # The index lists every file on disk: a fund that failed this run keeps its
    # last data instead of vanishing from the index.
    keys = sorted(f[:-5] for f in os.listdir(DIST_DIR) if f.endswith(".json"))
    aliases = {k: v for k, v in sorted(aliases.items()) if v in keys and k not in keys}
    print(f"Saved {len(funds)} funds this run; index has {len(keys)} funds, {len(aliases)} aliases")
    update_index(INDEX_PATH, keys, key="funds", extra={"aliases": aliases})


if __name__ == "__main__":
    main()
