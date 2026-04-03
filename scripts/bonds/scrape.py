from __future__ import annotations

import os
import time

from shared.network import create_session, fetch_with_retry
from shared.io import save_json, update_index
from bonds.moex_iss import (
    LISTING_URL,
    BONDIZATION_URL,
    parse_securities_listing,
    parse_bondization,
)

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "bonds")


def main() -> None:
    session = create_session()

    print("Step 1: Fetching bond listing from MOEX ISS...")
    all_bonds = []
    start = 0
    while True:
        url = f"{LISTING_URL}?start={start}&iss.meta=off"
        resp = fetch_with_retry(session, url)
        if not resp:
            print(f"ERROR: Could not fetch listing at start={start}. Aborting.")
            return
        bonds = parse_securities_listing(resp.json())
        if not bonds:
            break
        all_bonds.extend(bonds)
        start += len(bonds)
        print(f"  Fetched {len(all_bonds)} bonds so far...")

    seen = set()
    unique_bonds = []
    for b in all_bonds:
        if b["secid"] not in seen:
            seen.add(b["secid"])
            unique_bonds.append(b)
    print(f"  Total unique bonds: {len(unique_bonds)}")

    print("Step 2: Fetching bondization for each bond...")
    successful = []
    for i, bond in enumerate(unique_bonds, 1):
        secid = bond["secid"]
        if i % 100 == 0 or i == 1:
            print(f"  [{i}/{len(unique_bonds)}] {secid}...")

        url = BONDIZATION_URL.format(secid=secid)
        resp = fetch_with_retry(session, url)
        if not resp:
            continue

        try:
            bondization = parse_bondization(resp.json(), secid)

            result = {
                "secid": secid,
                "isin": bond.get("isin"),
                "name": bond.get("name"),
                "faceValue": bond.get("faceValue"),
                "currency": bond.get("currency"),
                "matDate": bond.get("matDate"),
                "scrapedAt": bondization["scrapedAt"],
                "coupons": bondization["coupons"],
                "amortizations": bondization["amortizations"],
                "offers": bondization["offers"],
            }

            path = os.path.join(DATA_DIR, "coupons", f"{secid}.json")
            save_json(path, result)
            successful.append(secid)
        except (ValueError, KeyError) as exc:
            print(f"  Error for {secid}: {exc}")

        if i % 10 == 0:
            time.sleep(0.5)

    print(f"Step 3: Updating index ({len(successful)}/{len(unique_bonds)} bonds)...")
    update_index(os.path.join(DATA_DIR, "index.json"), successful, key="securities")
    print("Done.")


if __name__ == "__main__":
    main()
