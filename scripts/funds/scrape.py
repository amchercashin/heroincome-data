from __future__ import annotations

import os

from shared.network import create_session, fetch_with_retry
from shared.io import save_json, update_index
from funds.parus import PARUS_FUNDS, SHEETS_CSV_URL, parse_parus_csv

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "funds")


def main() -> None:
    session = create_session()

    print(f"Fetching distributions for {len(PARUS_FUNDS)} Parus funds...")
    successful = []

    for fund in PARUS_FUNDS:
        name = fund["name"]
        print(f"  {name}...")
        url = SHEETS_CSV_URL.format(sheet_id=fund["sheetId"])
        resp = fetch_with_retry(session, url)
        if not resp:
            print(f"  Skipping {name} (fetch failed)")
            continue

        try:
            csv_text = resp.content.decode("utf-8")
            data = parse_parus_csv(
                csv_text,
                name=name,
                isin=fund["isin"],
                ticker=fund.get("ticker"),
            )
            filename = fund.get("ticker") or fund["isin"]
            path = os.path.join(DATA_DIR, "distributions", f"{filename}.json")
            save_json(path, data)
            successful.append(filename)
            print(f"  OK — {len(data['distributions'])} distributions")
        except (ValueError, KeyError) as exc:
            print(f"  Parse error for {name}: {exc}")

    print(f"Updating index ({len(successful)}/{len(PARUS_FUNDS)} funds)...")
    update_index(os.path.join(DATA_DIR, "index.json"), successful, key="funds")
    print("Done.")


if __name__ == "__main__":
    main()
