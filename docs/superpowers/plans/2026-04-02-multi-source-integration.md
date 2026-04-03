# Multi-Source Data Integration — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expand heroincome-data from a single-source stock dividend scraper to a multi-source pipeline covering stocks (dohod.ru + smartlab.ru), bonds (MOEX ISS), and funds (Parus Google Sheets).

**Architecture:** Separate pipeline per instrument type (`stocks/`, `bonds/`, `funds/`), each with its own scraper, parser, tests, data directory, and CI workflow. Common utilities (HTTP retry, JSON I/O, date parsing) live in `scripts/shared/`. Each phase is a standalone PR.

**Tech Stack:** Python 3.12, requests, beautifulsoup4, pytest, csv (stdlib), GitHub Actions

**Spec:** `docs/superpowers/specs/2026-04-02-multi-source-integration-design.md`

---

## Phase 1: Refactoring — shared utils + stocks restructure

**Goal:** Extract reusable code into `scripts/shared/`, move stock-specific code to `scripts/stocks/`, relocate data to `data/stocks/`. Zero behavior change — existing tests pass with new structure.

### Task 1.1: Create `scripts/shared/network.py`

**Files:**
- Create: `scripts/shared/__init__.py`
- Create: `scripts/shared/network.py`
- Create: `scripts/shared/test_shared.py`

- [ ] **Step 1: Write failing tests for network utilities**

Create `scripts/shared/test_shared.py`:

```python
from unittest.mock import MagicMock
from shared.network import fetch_with_retry, create_session


def test_create_session_sets_user_agent():
    session = create_session()
    assert "heroincome-data" in session.headers["User-Agent"]


def test_fetch_with_retry_returns_none_on_404():
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_session = MagicMock()
    mock_session.get.return_value = mock_response
    result = fetch_with_retry(mock_session, "https://example.com/404")
    assert result is None


def test_fetch_with_retry_returns_response_on_200():
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.raise_for_status.return_value = None
    mock_session = MagicMock()
    mock_session.get.return_value = mock_response
    result = fetch_with_retry(mock_session, "https://example.com/ok")
    assert result is mock_response
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd scripts && python -m pytest shared/test_shared.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'shared.network'`

- [ ] **Step 3: Implement shared network module**

Create `scripts/shared/__init__.py` (empty file).

Create `scripts/shared/network.py`:

```python
from __future__ import annotations

import time

import requests

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; heroincome-data/1.0; "
    "+https://github.com/amchercashin/heroincome-data)"
}


def create_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    return session


def fetch_with_retry(
    session: requests.Session, url: str, retries: int = 3
) -> requests.Response | None:
    """Fetch URL with exponential backoff. Returns None on 404 or after exhausting retries."""
    delays = [1, 4, 16]
    for attempt in range(retries):
        try:
            response = session.get(url, timeout=30)
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd scripts && python -m pytest shared/test_shared.py -v`
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add scripts/shared/__init__.py scripts/shared/network.py scripts/shared/test_shared.py
git commit -m "refactor: extract shared network utilities from scrape.py"
```

### Task 1.2: Create `scripts/shared/io.py` and `scripts/shared/dates.py`

**Files:**
- Create: `scripts/shared/io.py`
- Create: `scripts/shared/dates.py`
- Modify: `scripts/shared/test_shared.py`

- [ ] **Step 1: Write failing tests for io and dates**

Append to `scripts/shared/test_shared.py`:

```python
import json
import os
import tempfile
from shared.io import save_json, update_index
from shared.dates import parse_date_dmy, parse_date_iso


def test_save_json_creates_file_with_content():
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "sub", "test.json")
        save_json(path, {"key": "value"})
        with open(path, encoding="utf-8") as f:
            assert json.load(f) == {"key": "value"}


def test_save_json_handles_cyrillic():
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "test.json")
        save_json(path, {"name": "Лукойл"})
        with open(path, encoding="utf-8") as f:
            content = f.read()
        assert "Лукойл" in content  # ensure_ascii=False


def test_update_index_creates_index_file():
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "index.json")
        update_index(path, ["SBER", "LKOH"])
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        assert data["tickersCount"] == 2
        assert data["tickers"] == ["LKOH", "SBER"]  # sorted
        assert "updatedAt" in data


def test_parse_date_dmy_valid():
    assert parse_date_dmy("21.11.2025") == "2025-11-21"


def test_parse_date_dmy_invalid():
    assert parse_date_dmy("n/a") is None
    assert parse_date_dmy("") is None
    assert parse_date_dmy("  ") is None


def test_parse_date_iso_valid():
    assert parse_date_iso("2025-11-21") == "2025-11-21"


def test_parse_date_iso_invalid():
    assert parse_date_iso("not-a-date") is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd scripts && python -m pytest shared/test_shared.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'shared.io'`

- [ ] **Step 3: Implement io and dates modules**

Create `scripts/shared/io.py`:

```python
from __future__ import annotations

import json
import os
from datetime import datetime, timezone


def save_json(path: str, data: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def update_index(path: str, items: list[str], key: str = "tickers") -> None:
    index = {
        "updatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        f"{key}Count": len(items),
        key: sorted(items),
    }
    save_json(path, index)
```

Create `scripts/shared/dates.py`:

```python
from __future__ import annotations

from datetime import datetime


def parse_date_dmy(text: str) -> str | None:
    """Parse DD.MM.YYYY -> YYYY-MM-DD. Returns None for blank/n/a."""
    text = text.strip()
    if not text or text.lower() == "n/a":
        return None
    try:
        return datetime.strptime(text, "%d.%m.%Y").strftime("%Y-%m-%d")
    except ValueError:
        return None


def parse_date_iso(text: str) -> str | None:
    """Validate and normalize YYYY-MM-DD. Returns None for invalid."""
    text = text.strip()
    if not text or text.lower() == "n/a":
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        return None
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd scripts && python -m pytest shared/test_shared.py -v`
Expected: 10 passed

- [ ] **Step 5: Commit**

```bash
git add scripts/shared/io.py scripts/shared/dates.py scripts/shared/test_shared.py
git commit -m "refactor: add shared io and dates utilities"
```

### Task 1.3: Create `scripts/stocks/dohod.py` — extract dohod parser

**Files:**
- Create: `scripts/stocks/__init__.py`
- Create: `scripts/stocks/dohod.py`
- Create: `scripts/stocks/test_stocks.py`

- [ ] **Step 1: Write failing tests**

Create `scripts/stocks/__init__.py` (empty file).

Create `scripts/stocks/test_stocks.py` — adapted from existing `test_scrape.py`, importing from new locations:

```python
from stocks.dohod import parse_tickers_from_index, parse_dividend_page

INDEX_HTML = """
<html><body>
<table>
  <tr><td><a href="/ik/analytics/dividend/lkoh">ЛУКОЙЛ</a></td></tr>
  <tr><td><a href="/ik/analytics/dividend/sber">Сбербанк</a></td></tr>
  <tr><td><a href="/ik/analytics/dividend/sberp">Сбербанк-П</a></td></tr>
  <tr><td><a href="/other/link">Другое</a></td></tr>
</table>
</body></html>
"""

DIVIDEND_HTML = """
<html><body>
<table class="content-table"><tr><th>По годам</th></tr><tr><td>данные</td></tr></table>
<table class="content-table">
  <tr>
    <th>Дата объявления дивиденда</th>
    <th>Дата закрытия реестра</th>
    <th>Год для учета дивиденда</th>
    <th>Дивиденд</th>
  </tr>
  <tr>
    <td>21.11.2025</td>
    <td>12.01.2026</td>
    <td>2025</td>
    <td>397</td>
  </tr>
  <tr>
    <td>03.06.2025</td>
    <td>17.07.2025</td>
    <td>2024</td>
    <td>514</td>
  </tr>
  <tr class="forecast">
    <td><img src="x.png"> n/a </td>
    <td>04.05.2026 <img src="i.png"></td>
    <td> n/a </td>
    <td>278</td>
  </tr>
</table>
</body></html>
"""


def test_parse_tickers_from_index_returns_uppercase_tickers():
    tickers = parse_tickers_from_index(INDEX_HTML)
    assert "LKOH" in tickers
    assert "SBER" in tickers
    assert "SBERP" in tickers


def test_parse_tickers_from_index_excludes_non_dividend_links():
    tickers = parse_tickers_from_index(INDEX_HTML)
    assert all("/" not in t for t in tickers)


def test_parse_dividend_page_returns_correct_structure():
    result = parse_dividend_page(DIVIDEND_HTML, "LKOH")
    assert result["ticker"] == "LKOH"
    assert "scrapedAt" in result
    assert isinstance(result["payments"], list)


def test_parse_dividend_page_parses_fact_row():
    result = parse_dividend_page(DIVIDEND_HTML, "LKOH")
    facts = [p for p in result["payments"] if not p["isForecast"]]
    lkoh_2025 = next(p for p in facts if p["recordDate"] == "2026-01-12")
    assert lkoh_2025["amount"] == 397.0
    assert lkoh_2025["declaredDate"] == "2025-11-21"
    assert lkoh_2025["year"] == 2025


def test_parse_dividend_page_parses_forecast_row():
    result = parse_dividend_page(DIVIDEND_HTML, "LKOH")
    forecasts = [p for p in result["payments"] if p["isForecast"]]
    assert len(forecasts) == 1
    f = forecasts[0]
    assert f["recordDate"] == "2026-05-04"
    assert f["amount"] == 278.0
    assert f["declaredDate"] is None
    assert f["year"] is None


def test_parse_dividend_page_handles_img_tags_in_cells():
    result = parse_dividend_page(DIVIDEND_HTML, "LKOH")
    forecast = next(p for p in result["payments"] if p["isForecast"])
    assert forecast["recordDate"] == "2026-05-04"
    assert forecast["declaredDate"] is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd scripts && python -m pytest stocks/test_stocks.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'stocks.dohod'`

- [ ] **Step 3: Implement stocks/dohod.py**

Create `scripts/stocks/dohod.py` — extracted from `scripts/scrape.py`, using shared utilities:

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd scripts && python -m pytest stocks/test_stocks.py -v`
Expected: 6 passed

- [ ] **Step 5: Commit**

```bash
git add scripts/stocks/__init__.py scripts/stocks/dohod.py scripts/stocks/test_stocks.py
git commit -m "refactor: extract dohod.ru parser into scripts/stocks/dohod.py"
```

### Task 1.4: Create `scripts/stocks/scrape.py` entry point

**Files:**
- Create: `scripts/stocks/scrape.py`

- [ ] **Step 1: Write the stocks entry point**

Create `scripts/stocks/scrape.py`:

```python
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
```

- [ ] **Step 2: Verify all tests still pass**

Run: `cd scripts && python -m pytest shared/test_shared.py stocks/test_stocks.py -v`
Expected: All passed

- [ ] **Step 3: Commit**

```bash
git add scripts/stocks/scrape.py
git commit -m "refactor: add stocks scrape.py entry point using shared utilities"
```

### Task 1.5: Move data directories and update CI

**Files:**
- Move: `data/dividends/` -> `data/stocks/dividends/`
- Move: `data/index.json` -> `data/stocks/index.json`
- Modify: `.github/workflows/update-dividends.yml`
- Delete: `scripts/scrape.py`
- Delete: `scripts/test_scrape.py`

- [ ] **Step 1: Move data directories**

```bash
mkdir -p data/stocks
git mv data/dividends data/stocks/dividends
git mv data/index.json data/stocks/index.json
```

- [ ] **Step 2: Update CI workflow**

Replace `.github/workflows/update-dividends.yml` with:

```yaml
name: Update Stock Dividends

on:
  schedule:
    - cron: '0 9 1,15 * *'
  workflow_dispatch:

jobs:
  scrape:
    runs-on: ubuntu-latest
    permissions:
      contents: write

    steps:
      - name: Checkout repo
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.12'

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Run scraper
        run: cd scripts && python -m stocks.scrape

      - name: Commit updated data (if changed)
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add data/stocks/
          if git diff --staged --quiet; then
            echo "No data changes — skipping commit"
          else
            git commit -m "chore: update stock dividends $(date -u +%Y-%m-%d)"
            git push
          fi
```

- [ ] **Step 3: Delete legacy files**

```bash
git rm scripts/scrape.py scripts/test_scrape.py
```

- [ ] **Step 4: Run all tests to verify nothing broke**

Run: `cd scripts && python -m pytest shared/ stocks/ -v`
Expected: All passed

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/update-dividends.yml data/stocks/ scripts/scrape.py scripts/test_scrape.py
git commit -m "refactor: restructure project — stocks pipeline with shared utilities

Move data/dividends/ -> data/stocks/dividends/
Delete legacy scripts/scrape.py
Update CI workflow for new structure"
```

- [ ] **Step 6: Sync prompt for heroincome app**

After merging this PR, provide this prompt for use in the heroincome app context:

> The heroincome-data repo has been restructured. Data paths changed:
> - `data/dividends/{TICKER}.json` -> `data/stocks/dividends/{TICKER}.json`
> - `data/index.json` -> `data/stocks/index.json`
>
> The JSON schema inside each file is unchanged. Update all paths in code that reads from heroincome-data to use the new `data/stocks/` prefix.

---

## Phase 2: Bonds pipeline (MOEX ISS API)

**Goal:** Add bond coupon data collection from the free MOEX ISS API. Covers ~3000 active bonds with coupon schedules, amortizations, and offers.

### Task 2.1: Create `scripts/bonds/moex_iss.py` — MOEX ISS parser

**Files:**
- Create: `scripts/bonds/__init__.py`
- Create: `scripts/bonds/moex_iss.py`
- Create: `scripts/bonds/test_bonds.py`

- [ ] **Step 1: Write failing tests for MOEX ISS parser**

Create `scripts/bonds/__init__.py` (empty file).

Create `scripts/bonds/test_bonds.py`:

```python
from bonds.moex_iss import parse_securities_listing, parse_bondization

LISTING_RESPONSE = {
    "securities": {
        "columns": ["SECID", "ISIN", "SHORTNAME", "FACEVALUE", "FACEUNIT",
                     "MATDATE", "COUPONVALUE", "NEXTCOUPON", "COUPONPERIOD",
                     "SECTYPE"],
        "data": [
            ["SU26238RMFS4", "RU000A1038V6", "ОФЗ 26238", 1000.0, "SUR",
             "2041-05-28", 33.91, "2026-06-04", 182, "3"],
            ["RU000A106T36", "RU000A106T36", "Сбер Б 002Р-20R", 1000.0, "SUR",
             "2027-04-15", 0.0, "2026-07-17", 91, "6"],
        ],
    }
}

BONDIZATION_RESPONSE = {
    "coupons": {
        "columns": ["isin", "name", "issuevalue", "coupondate", "recorddate",
                     "startdate", "initialfacevalue", "facevalue", "faceunit",
                     "value", "valueprc", "value_rub", "secid", "primary_boardid"],
        "data": [
            ["RU000A1038V6", "ОФЗ 26238", 500000000000, "2019-11-20",
             "2019-11-19", "2019-05-22", 1000.0, 1000.0, "SUR",
             33.91, 6.9, 33.91, "SU26238RMFS4", "TQOB"],
            ["RU000A1038V6", "ОФЗ 26238", 500000000000, "2020-05-20",
             "2020-05-19", "2019-11-20", 1000.0, 1000.0, "SUR",
             33.91, 6.9, 33.91, "SU26238RMFS4", "TQOB"],
        ],
    },
    "amortizations": {
        "columns": ["isin", "name", "issuevalue", "amortdate", "facevalue",
                     "initialfacevalue", "faceunit", "value", "valueprc",
                     "value_rub", "data_source", "secid", "primary_boardid"],
        "data": [
            ["RU000A1038V6", "ОФЗ 26238", 500000000000, "2041-05-28",
             1000.0, 1000.0, "SUR", 1000.0, 100.0, 1000.0,
             "maturity", "SU26238RMFS4", "TQOB"],
        ],
    },
    "offers": {
        "columns": ["isin", "name", "issuevalue", "offerdate", "offerdatestart",
                     "offerdateend", "facevalue", "faceunit", "price", "value",
                     "agent", "offertype", "secid", "primary_boardid"],
        "data": [],
    },
}


def test_parse_securities_listing_extracts_bonds():
    bonds = parse_securities_listing(LISTING_RESPONSE)
    assert len(bonds) == 2
    assert bonds[0]["secid"] == "SU26238RMFS4"
    assert bonds[0]["isin"] == "RU000A1038V6"
    assert bonds[0]["name"] == "ОФЗ 26238"
    assert bonds[0]["faceValue"] == 1000.0
    assert bonds[0]["currency"] == "SUR"
    assert bonds[0]["matDate"] == "2041-05-28"


def test_parse_securities_listing_handles_empty():
    empty = {"securities": {"columns": LISTING_RESPONSE["securities"]["columns"], "data": []}}
    assert parse_securities_listing(empty) == []


def test_parse_bondization_extracts_coupons():
    result = parse_bondization(BONDIZATION_RESPONSE, "SU26238RMFS4")
    assert len(result["coupons"]) == 2
    c = result["coupons"][0]
    assert c["couponDate"] == "2019-11-20"
    assert c["recordDate"] == "2019-11-19"
    assert c["value"] == 33.91
    assert c["valuePrc"] == 6.9
    assert c["startDate"] == "2019-05-22"


def test_parse_bondization_extracts_amortizations():
    result = parse_bondization(BONDIZATION_RESPONSE, "SU26238RMFS4")
    assert len(result["amortizations"]) == 1
    a = result["amortizations"][0]
    assert a["amortDate"] == "2041-05-28"
    assert a["value"] == 1000.0
    assert a["valuePrc"] == 100.0
    assert a["type"] == "maturity"


def test_parse_bondization_handles_empty_offers():
    result = parse_bondization(BONDIZATION_RESPONSE, "SU26238RMFS4")
    assert result["offers"] == []


def test_parse_bondization_handles_null_coupon_value():
    """Floater bonds have null coupon values for future dates."""
    data = {
        "coupons": {
            "columns": BONDIZATION_RESPONSE["coupons"]["columns"],
            "data": [
                ["RU000A106T36", "Сбер Б", 10000000000, "2026-07-17",
                 "2026-07-16", "2026-04-17", 1000.0, 1000.0, "SUR",
                 None, None, None, "RU000A106T36", "TQCB"],
            ],
        },
        "amortizations": {"columns": [], "data": []},
        "offers": {"columns": [], "data": []},
    }
    result = parse_bondization(data, "RU000A106T36")
    assert result["coupons"][0]["value"] is None
    assert result["coupons"][0]["valuePrc"] is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd scripts && python -m pytest bonds/test_bonds.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'bonds.moex_iss'`

- [ ] **Step 3: Implement MOEX ISS parser**

Create `scripts/bonds/moex_iss.py`:

```python
from __future__ import annotations

from datetime import datetime, timezone

ISS_BASE = "https://iss.moex.com/iss"
LISTING_URL = f"{ISS_BASE}/engines/stock/markets/bonds/securities.json"
BONDIZATION_URL = f"{ISS_BASE}/securities/{{secid}}/bondization.json?limit=200"
SECURITY_URL = f"{ISS_BASE}/securities/{{secid}}.json"


def _columns_data_to_dicts(block: dict) -> list[dict]:
    """Convert MOEX ISS {columns: [...], data: [[...], ...]} to list of dicts."""
    columns = block.get("columns", [])
    return [dict(zip(columns, row)) for row in block.get("data", [])]


def parse_securities_listing(response_json: dict) -> list[dict]:
    """Parse /engines/stock/markets/bonds/securities.json response."""
    rows = _columns_data_to_dicts(response_json.get("securities", {}))
    bonds = []
    for row in rows:
        bonds.append(
            {
                "secid": row["SECID"],
                "isin": row.get("ISIN"),
                "name": row.get("SHORTNAME"),
                "faceValue": row.get("FACEVALUE"),
                "currency": row.get("FACEUNIT"),
                "matDate": row.get("MATDATE"),
            }
        )
    return bonds


def parse_bondization(response_json: dict, secid: str) -> dict:
    """Parse /securities/{secid}/bondization.json response."""
    coupon_rows = _columns_data_to_dicts(response_json.get("coupons", {}))
    amort_rows = _columns_data_to_dicts(response_json.get("amortizations", {}))
    offer_rows = _columns_data_to_dicts(response_json.get("offers", {}))

    coupons = [
        {
            "couponDate": r.get("coupondate"),
            "recordDate": r.get("recorddate"),
            "value": r.get("value"),
            "valuePrc": r.get("valueprc"),
            "startDate": r.get("startdate"),
        }
        for r in coupon_rows
    ]

    amortizations = [
        {
            "amortDate": r.get("amortdate"),
            "value": r.get("value"),
            "valuePrc": r.get("valueprc"),
            "type": r.get("data_source", "amortization"),
        }
        for r in amort_rows
    ]

    offers = [
        {
            "offerDate": r.get("offerdate"),
            "offerType": r.get("offertype"),
            "value": r.get("value"),
        }
        for r in offer_rows
    ]

    return {
        "secid": secid,
        "scrapedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "coupons": coupons,
        "amortizations": amortizations,
        "offers": offers,
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd scripts && python -m pytest bonds/test_bonds.py -v`
Expected: 6 passed

- [ ] **Step 5: Commit**

```bash
git add scripts/bonds/__init__.py scripts/bonds/moex_iss.py scripts/bonds/test_bonds.py
git commit -m "feat: add MOEX ISS bond coupon parser with tests"
```

### Task 2.2: Create `scripts/bonds/scrape.py` entry point

**Files:**
- Create: `scripts/bonds/scrape.py`

- [ ] **Step 1: Implement bonds entry point**

Create `scripts/bonds/scrape.py`:

```python
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
    # Paginate through all bonds (MOEX returns up to 100 per page by default)
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

    # Deduplicate by secid (same bond can appear on multiple boards)
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

            # Merge listing metadata + bondization
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

        # Gentle rate limiting for MOEX ISS
        if i % 10 == 0:
            time.sleep(0.5)

    print(f"Step 3: Updating index ({len(successful)}/{len(unique_bonds)} bonds)...")
    update_index(os.path.join(DATA_DIR, "index.json"), successful, key="securities")
    print("Done.")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Verify all tests pass**

Run: `cd scripts && python -m pytest bonds/ shared/ -v`
Expected: All passed

- [ ] **Step 3: Commit**

```bash
git add scripts/bonds/scrape.py
git commit -m "feat: add bonds scrape.py entry point for MOEX ISS"
```

### Task 2.3: Create bonds CI workflow

**Files:**
- Create: `.github/workflows/update-bonds.yml`

- [ ] **Step 1: Write workflow**

Create `.github/workflows/update-bonds.yml`:

```yaml
name: Update Bond Coupons

on:
  schedule:
    - cron: '0 9 * * 1'    # Every Monday at 09:00 UTC
  workflow_dispatch:

jobs:
  scrape:
    runs-on: ubuntu-latest
    permissions:
      contents: write

    steps:
      - name: Checkout repo
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.12'

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Run scraper
        run: cd scripts && python -m bonds.scrape

      - name: Commit updated data (if changed)
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add data/bonds/
          if git diff --staged --quiet; then
            echo "No data changes — skipping commit"
          else
            git commit -m "chore: update bond coupons $(date -u +%Y-%m-%d)"
            git push
          fi
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/update-bonds.yml
git commit -m "ci: add weekly bond coupon update workflow"
```

- [ ] **Step 3: Sync prompt for heroincome app**

> New data added to heroincome-data: **bond coupons**.
> - Path: `data/bonds/coupons/{SECID}.json`
> - Index: `data/bonds/index.json` (key: `securities`, not `tickers`)
> - Schema: `{ secid, isin, name, faceValue, currency, matDate, scrapedAt, coupons: [{ couponDate, recordDate, value, valuePrc, startDate }], amortizations: [{ amortDate, value, valuePrc, type }], offers: [{ offerDate, offerType, value }] }`
> - `value` can be `null` for floater bonds (future coupons)
> - Updated weekly (Mondays)
>
> Add bond coupon data consumption to the app. The `secid` is the primary identifier (MOEX-native). `isin` is optional.

---

## Phase 3: Funds pipeline (Parus Google Sheets)

**Goal:** Add fund distribution data collection from Parus management company's public Google Sheets. Covers 8 funds with monthly income distributions.

### Task 3.1: Create `scripts/funds/parus.py` — CSV parser

**Files:**
- Create: `scripts/funds/__init__.py`
- Create: `scripts/funds/parus.py`
- Create: `scripts/funds/test_funds.py`

- [ ] **Step 1: Write failing tests**

Create `scripts/funds/__init__.py` (empty file).

Create `scripts/funds/test_funds.py`:

```python
from funds.parus import parse_parus_csv, PARUS_FUNDS

CSV_DATA = """Дата выплаты ежемесячного дохода,Закрытие реестра УК,Стоимость пая (RUB),Доход на 1 пай до НДФЛ (RUB),Доход после НДФЛ 13% на 1 пай (RUB)
15.03.2026,10.03.2026,"1 250,00","9,52 (0,76%)","8,28"
15.02.2026,10.02.2026,"1 245,00","9,10 (0,73%)","7,92"
~15.04.2026 (план),~10.04.2026,"1 260,00","9,80 (0,78%)","8,53"
"""

CSV_EMPTY = """Дата выплаты ежемесячного дохода,Закрытие реестра УК,Стоимость пая (RUB),Доход на 1 пай до НДФЛ (RUB),Доход после НДФЛ 13% на 1 пай (RUB)
"""


def test_parse_parus_csv_extracts_distributions():
    result = parse_parus_csv(CSV_DATA, "ПАРУС-ОЗН", isin="RU000A1022Z1", ticker="PLZ5")
    assert len(result["distributions"]) == 3
    assert result["name"] == "ПАРУС-ОЗН"
    assert result["isin"] == "RU000A1022Z1"
    assert result["ticker"] == "PLZ5"
    assert result["managementCompany"] == "Parus"


def test_parse_parus_csv_parses_paid_row():
    result = parse_parus_csv(CSV_DATA, "ПАРУС-ОЗН", isin="RU000A1022Z1")
    paid = [d for d in result["distributions"] if d["status"] == "paid"]
    assert len(paid) == 2
    d = paid[0]
    assert d["paymentDate"] == "2026-03-15"
    assert d["recordDate"] == "2026-03-10"
    assert d["unitPrice"] == 1250.0
    assert d["amountBeforeTax"] == 9.52
    assert d["amountAfterTax"] == 8.28
    assert d["yieldPrc"] == 0.76


def test_parse_parus_csv_parses_planned_row():
    result = parse_parus_csv(CSV_DATA, "ПАРУС-ОЗН", isin="RU000A1022Z1")
    planned = [d for d in result["distributions"] if d["status"] == "planned"]
    assert len(planned) == 1
    p = planned[0]
    assert p["paymentDate"] == "2026-04-15"
    assert p["amountBeforeTax"] == 9.80


def test_parse_parus_csv_handles_empty():
    result = parse_parus_csv(CSV_EMPTY, "ПАРУС-ОЗН", isin="RU000A1022Z1")
    assert result["distributions"] == []


def test_parus_funds_has_8_entries():
    assert len(PARUS_FUNDS) == 8


def test_parse_parus_csv_yield_extraction():
    result = parse_parus_csv(CSV_DATA, "TEST", isin="TEST")
    d = result["distributions"][0]
    assert d["yieldPrc"] == 0.76
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd scripts && python -m pytest funds/test_funds.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'funds.parus'`

- [ ] **Step 3: Implement Parus CSV parser**

Create `scripts/funds/parus.py`:

```python
from __future__ import annotations

import csv
import io
import re
from datetime import datetime, timezone

from shared.dates import parse_date_dmy

SHEETS_CSV_URL = "https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"

PARUS_FUNDS = [
    {"name": "ПАРУС-ОЗН", "isin": "RU000A1022Z1", "ticker": "PLZ5",
     "sheetId": "1EBBlo_L-h1X1zkvybI-cPh-gPjS4mUbJ4NkrkLTeRd4"},
    {"name": "ПАРУС-СБЛ", "isin": "RU000A104172", "ticker": None,
     "sheetId": "1dcOHCw6t2C2BnQep4x6-Dc71VivN03JpbgQ6KBP3Uvw"},
    {"name": "ПАРУС-НОРДВЕЙ", "isin": "RU000A104KU3", "ticker": None,
     "sheetId": "14ImcLbbh8wSVwiohuYbpJIS04jfV3x-GdXTrIXeo1FU"},
    {"name": "ПАРУС-ЛОГИСТИКА", "isin": "RU000A105328", "ticker": None,
     "sheetId": "1_Jqoal_hmJ0jpDHutasR5QarJfPD8RhXYb3QdcmMBak"},
    {"name": "ПАРУС-ДВИНЦЕВ", "isin": "RU000A1068X9", "ticker": None,
     "sheetId": "1G1Eusuay0PU4aYYohxI3jMl1bJYok8bsTBajNeqx5bI"},
    {"name": "ПАРУС-КРАСНОЯРСК", "isin": "RU000A108UH0", "ticker": None,
     "sheetId": "1RRQwzPScXeaQ7TXiOmeIjgnxoy4y9mkIgqHfJIhw18Y"},
    {"name": "ПАРУС-ЗОЛЯ", "isin": "RU000A10CFM8", "ticker": None,
     "sheetId": "1hQGBzKvDNHB0tnO0DrC0MNsgrYuuULjeGIVHfXyL_w4"},
    {"name": "ПАРУС-ТРИУМФ", "isin": "XTRIUMF", "ticker": None,
     "sheetId": "1Egct-_5Bbi_LsHyidqL55i8EkT_nFvYgQ__hC6XyEDw"},
]


def _parse_russian_float(text: str) -> float | None:
    """Parse '1 250,00' or '9,52' -> float. Returns None for blank."""
    text = text.strip()
    if not text:
        return None
    # Remove spaces (thousand separators) and replace comma with period
    cleaned = text.replace("\xa0", "").replace(" ", "").replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_yield(text: str) -> float | None:
    """Extract yield from '9,52 (0,76%)' -> 0.76. Returns None if not found."""
    match = re.search(r"\((\d+[,.]?\d*)\s*%\)", text)
    if not match:
        return None
    return _parse_russian_float(match.group(1))


def _parse_amount_before_tax(text: str) -> float | None:
    """Extract amount from '9,52 (0,76%)' -> 9.52."""
    # Remove the yield part in parentheses
    cleaned = re.sub(r"\(.*?\)", "", text).strip()
    return _parse_russian_float(cleaned)


def _clean_planned(text: str) -> tuple[str, bool]:
    """Remove plan markers. Returns (cleaned_text, is_planned)."""
    is_planned = "(план)" in text or text.strip().startswith("~")
    cleaned = text.replace("(план)", "").replace("~", "").strip()
    return cleaned, is_planned


def parse_parus_csv(
    csv_text: str,
    name: str,
    isin: str | None = None,
    ticker: str | None = None,
) -> dict:
    """Parse Parus fund CSV export into structured data."""
    reader = csv.reader(io.StringIO(csv_text))
    header = next(reader, None)
    if not header:
        return _empty_result(name, isin, ticker)

    distributions: list[dict] = []
    for row in reader:
        if len(row) < 5:
            continue

        payment_raw, record_raw, price_raw, before_tax_raw, after_tax_raw = (
            row[0], row[1], row[2], row[3], row[4]
        )

        payment_clean, is_planned = _clean_planned(payment_raw)
        record_clean, _ = _clean_planned(record_raw)

        payment_date = parse_date_dmy(payment_clean)
        record_date = parse_date_dmy(record_clean)

        if not payment_date:
            continue

        distributions.append(
            {
                "paymentDate": payment_date,
                "recordDate": record_date,
                "unitPrice": _parse_russian_float(price_raw),
                "amountBeforeTax": _parse_amount_before_tax(before_tax_raw),
                "amountAfterTax": _parse_russian_float(after_tax_raw),
                "yieldPrc": _parse_yield(before_tax_raw),
                "status": "planned" if is_planned else "paid",
            }
        )

    return {
        "isin": isin,
        "ticker": ticker,
        "name": name,
        "managementCompany": "Parus",
        "scrapedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "distributions": distributions,
    }


def _empty_result(name: str, isin: str | None, ticker: str | None) -> dict:
    return {
        "isin": isin,
        "ticker": ticker,
        "name": name,
        "managementCompany": "Parus",
        "scrapedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "distributions": [],
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd scripts && python -m pytest funds/test_funds.py -v`
Expected: 6 passed

- [ ] **Step 5: Commit**

```bash
git add scripts/funds/__init__.py scripts/funds/parus.py scripts/funds/test_funds.py
git commit -m "feat: add Parus fund CSV parser with tests"
```

### Task 3.2: Create `scripts/funds/scrape.py` entry point

**Files:**
- Create: `scripts/funds/scrape.py`

- [ ] **Step 1: Implement funds entry point**

Create `scripts/funds/scrape.py`:

```python
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
            data = parse_parus_csv(
                resp.text,
                name=name,
                isin=fund["isin"],
                ticker=fund.get("ticker"),
            )
            # Use ticker for filename if available, otherwise ISIN
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
```

- [ ] **Step 2: Verify all tests pass**

Run: `cd scripts && python -m pytest funds/ shared/ -v`
Expected: All passed

- [ ] **Step 3: Commit**

```bash
git add scripts/funds/scrape.py
git commit -m "feat: add funds scrape.py entry point for Parus Google Sheets"
```

### Task 3.3: Create funds CI workflow

**Files:**
- Create: `.github/workflows/update-funds.yml`

- [ ] **Step 1: Write workflow**

Create `.github/workflows/update-funds.yml`:

```yaml
name: Update Fund Distributions

on:
  schedule:
    - cron: '0 9 5 * *'    # 5th of each month at 09:00 UTC
  workflow_dispatch:

jobs:
  scrape:
    runs-on: ubuntu-latest
    permissions:
      contents: write

    steps:
      - name: Checkout repo
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.12'

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Run scraper
        run: cd scripts && python -m funds.scrape

      - name: Commit updated data (if changed)
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add data/funds/
          if git diff --staged --quiet; then
            echo "No data changes — skipping commit"
          else
            git commit -m "chore: update fund distributions $(date -u +%Y-%m-%d)"
            git push
          fi
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/update-funds.yml
git commit -m "ci: add monthly fund distributions update workflow"
```

- [ ] **Step 3: Sync prompt for heroincome app**

> New data added to heroincome-data: **fund distributions** (Parus ZPIFs).
> - Path: `data/funds/distributions/{TICKER_or_ISIN}.json`
> - Index: `data/funds/index.json` (key: `funds`)
> - Schema: `{ isin, ticker, name, managementCompany, scrapedAt, distributions: [{ paymentDate, recordDate, unitPrice, amountBeforeTax, amountAfterTax, yieldPrc, status }] }`
> - `ticker` and `isin` are both optional (not all funds have short tickers on MOEX)
> - `status`: `"paid"` or `"planned"`
> - Updated monthly (5th of each month)
> - Currently 8 Parus funds; more management companies will be added later.
>
> Add fund distribution data consumption to the app.

---

## Phase 4: Smartlab as second stock source

**Goal:** Add smartlab.ru as a secondary source for stock dividends. Implement merge logic to combine data from dohod.ru and smartlab.ru. Expand coverage from ~127 to ~222 tickers. Upgrade schema: `isForecast` -> `status`, add `sources` block, add optional `isin`.

### Task 4.1: Create `scripts/stocks/smartlab.py` — parser

**Files:**
- Create: `scripts/stocks/smartlab.py`
- Modify: `scripts/stocks/test_stocks.py`

- [ ] **Step 1: Write failing tests for Smartlab parser**

Append to `scripts/stocks/test_stocks.py`:

```python
from stocks.smartlab import parse_smartlab_tickers, parse_smartlab_dividend_page

SMARTLAB_INDEX_HTML = """
<html><body>
<script>
var aBubbleData = [
  {"secid":"LKOH","company_url":"/q/LKOH/","name":"ЛУКОЙЛ"},
  {"secid":"SBER","company_url":"/q/SBER/","name":"Сбербанк"},
  {"secid":"GAZP","company_url":"/q/GAZP/","name":"Газпром"}
];
</script>
</body></html>
"""

SMARTLAB_TICKER_HTML = """
<html><body>
<h2>Ожидаемые дивиденды</h2>
<table>
  <tr><th>Тикер</th><th>дата T-1</th><th>дата отсечки</th><th>Период</th>
      <th>дивиденд</th><th>Цена акции</th><th>Див. доходность</th></tr>
  <tr class="dividend_approved">
    <td>LKOH</td><td>11.01.2026</td><td>12.01.2026</td><td>2025</td>
    <td>397</td><td>7200</td><td>5,51%</td>
  </tr>
</table>
<h2>Выплаченные дивиденды</h2>
<table>
  <tr><th>Тикер</th><th>дата T-1</th><th>дата отсечки</th><th>Период</th>
      <th>дивиденд</th><th>Цена акции</th><th>Див. доходность</th></tr>
  <tr>
    <td>LKOH</td><td>16.07.2025</td><td>17.07.2025</td><td>2024</td>
    <td>514</td><td>6800</td><td>7,56%</td>
  </tr>
  <tr>
    <td>LKOH</td><td>02.06.2025</td><td>03.06.2025</td><td>2024</td>
    <td>541</td><td>7100</td><td>7,62%</td>
  </tr>
</table>
</body></html>
"""

SMARTLAB_FORECAST_HTML = """
<html><body>
<h2>Ожидаемые дивиденды</h2>
<table>
  <tr><th>Тикер</th><th>дата T-1</th><th>дата отсечки</th><th>Период</th>
      <th>дивиденд</th><th>Цена акции</th><th>Див. доходность</th></tr>
  <tr>
    <td>SBER</td><td>10.07.2026</td><td>11.07.2026</td><td>2025</td>
    <td>37,76</td><td>320</td><td>11,80%</td>
  </tr>
</table>
<h2>Выплаченные дивиденды</h2>
<table>
  <tr><th>Тикер</th><th>дата T-1</th><th>дата отсечки</th><th>Период</th>
      <th>дивиденд</th><th>Цена акции</th><th>Див. доходность</th></tr>
</table>
</body></html>
"""


def test_parse_smartlab_tickers_from_bubble_data():
    tickers = parse_smartlab_tickers(SMARTLAB_INDEX_HTML)
    assert "LKOH" in tickers
    assert "SBER" in tickers
    assert "GAZP" in tickers
    assert tickers == sorted(tickers)


def test_parse_smartlab_dividend_page_paid():
    result = parse_smartlab_dividend_page(SMARTLAB_TICKER_HTML, "LKOH")
    assert result["ticker"] == "LKOH"
    paid = [p for p in result["payments"] if p["status"] == "paid"]
    assert len(paid) == 2
    assert paid[0]["recordDate"] == "2025-07-17"
    assert paid[0]["amount"] == 514.0


def test_parse_smartlab_dividend_page_approved():
    result = parse_smartlab_dividend_page(SMARTLAB_TICKER_HTML, "LKOH")
    approved = [p for p in result["payments"] if p["status"] == "approved"]
    assert len(approved) == 1
    assert approved[0]["recordDate"] == "2026-01-12"
    assert approved[0]["amount"] == 397.0


def test_parse_smartlab_dividend_page_forecast():
    result = parse_smartlab_dividend_page(SMARTLAB_FORECAST_HTML, "SBER")
    forecasts = [p for p in result["payments"] if p["status"] == "forecast"]
    assert len(forecasts) == 1
    assert forecasts[0]["amount"] == 37.76
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd scripts && python -m pytest stocks/test_stocks.py::test_parse_smartlab_tickers_from_bubble_data -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'stocks.smartlab'`

- [ ] **Step 3: Implement Smartlab parser**

Create `scripts/stocks/smartlab.py`:

```python
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
                # Expected dividends table
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd scripts && python -m pytest stocks/test_stocks.py -v`
Expected: All passed (existing dohod tests + 4 new smartlab tests)

- [ ] **Step 5: Commit**

```bash
git add scripts/stocks/smartlab.py scripts/stocks/test_stocks.py
git commit -m "feat: add Smartlab dividend parser with tests"
```

### Task 4.2: Create `scripts/stocks/merge.py` — merge logic

**Files:**
- Create: `scripts/stocks/merge.py`
- Modify: `scripts/stocks/test_stocks.py`

- [ ] **Step 1: Write failing tests for merge logic**

Append to `scripts/stocks/test_stocks.py`:

```python
from stocks.merge import merge_payments

DOHOD_DATA = {
    "ticker": "LKOH",
    "payments": [
        {"recordDate": "2026-01-12", "declaredDate": "2025-11-21",
         "amount": 397.0, "year": 2025, "isForecast": False},
        {"recordDate": "2025-07-17", "declaredDate": "2025-06-03",
         "amount": 514.0, "year": 2024, "isForecast": False},
    ],
}

SMARTLAB_DATA = {
    "ticker": "LKOH",
    "payments": [
        {"recordDate": "2026-01-12", "amount": 397.0, "year": 2025,
         "status": "approved"},
        {"recordDate": "2025-07-17", "amount": 514.0, "year": 2024,
         "status": "paid"},
        {"recordDate": "2026-05-04", "amount": 278.0, "year": None,
         "status": "forecast"},
    ],
}


def test_merge_combines_matching_payments():
    result = merge_payments(DOHOD_DATA, SMARTLAB_DATA, "LKOH")
    # 2 from dohod matched with smartlab + 1 smartlab-only forecast
    assert len(result) == 3


def test_merge_takes_dohod_amount_as_priority():
    result = merge_payments(DOHOD_DATA, SMARTLAB_DATA, "LKOH")
    matched = next(p for p in result if p["recordDate"] == "2026-01-12")
    assert matched["amount"] == 397.0  # dohod is priority


def test_merge_preserves_sources():
    result = merge_payments(DOHOD_DATA, SMARTLAB_DATA, "LKOH")
    matched = next(p for p in result if p["recordDate"] == "2026-01-12")
    assert "dohod" in matched["sources"]
    assert "smartlab" in matched["sources"]
    assert matched["sources"]["dohod"]["amount"] == 397.0
    assert matched["sources"]["smartlab"]["amount"] == 397.0


def test_merge_status_priority():
    result = merge_payments(DOHOD_DATA, SMARTLAB_DATA, "LKOH")
    # dohod says paid (isForecast=False), smartlab says approved -> paid wins
    matched = next(p for p in result if p["recordDate"] == "2026-01-12")
    assert matched["status"] == "paid"


def test_merge_includes_smartlab_only_payments():
    result = merge_payments(DOHOD_DATA, SMARTLAB_DATA, "LKOH")
    forecast = next(p for p in result if p["recordDate"] == "2026-05-04")
    assert forecast["amount"] == 278.0
    assert forecast["status"] == "forecast"
    assert "smartlab" in forecast["sources"]
    assert "dohod" not in forecast["sources"]


def test_merge_dohod_only():
    result = merge_payments(DOHOD_DATA, None, "LKOH")
    assert len(result) == 2
    assert all("dohod" in p["sources"] for p in result)


def test_merge_smartlab_only():
    result = merge_payments(None, SMARTLAB_DATA, "LKOH")
    assert len(result) == 3
    assert all("smartlab" in p["sources"] for p in result)


def test_merge_logs_amount_discrepancy(capsys):
    dohod = {
        "ticker": "TEST",
        "payments": [
            {"recordDate": "2026-01-12", "declaredDate": None,
             "amount": 100.0, "year": 2025, "isForecast": False},
        ],
    }
    smartlab = {
        "ticker": "TEST",
        "payments": [
            {"recordDate": "2026-01-12", "amount": 105.0, "year": 2025,
             "status": "paid"},
        ],
    }
    result = merge_payments(dohod, smartlab, "TEST")
    assert result[0]["amount"] == 100.0  # dohod priority
    captured = capsys.readouterr()
    assert "discrepancy" in captured.out.lower() or "расхождение" in captured.out.lower()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd scripts && python -m pytest stocks/test_stocks.py::test_merge_combines_matching_payments -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'stocks.merge'`

- [ ] **Step 3: Implement merge logic**

Create `scripts/stocks/merge.py`:

```python
from __future__ import annotations

STATUS_PRIORITY = {"paid": 0, "approved": 1, "forecast": 2}


def _dohod_status(payment: dict) -> str:
    """Convert dohod.ru isForecast field to status string."""
    return "forecast" if payment.get("isForecast") else "paid"


def _best_status(statuses: list[str]) -> str:
    """Return the highest-priority status."""
    return min(statuses, key=lambda s: STATUS_PRIORITY.get(s, 99))


def merge_payments(
    dohod_data: dict | None,
    smartlab_data: dict | None,
    ticker: str,
) -> list[dict]:
    """Merge payments from dohod.ru and smartlab.ru.

    Priority: dohod.ru for amounts. Best (most certain) status wins.
    Matching by ticker + recordDate.
    """
    dohod_payments = (dohod_data or {}).get("payments", [])
    smartlab_payments = (smartlab_data or {}).get("payments", [])
    dohod_scraped = (dohod_data or {}).get("scrapedAt")
    smartlab_scraped = (smartlab_data or {}).get("scrapedAt")

    # Index smartlab by recordDate
    smartlab_by_date: dict[str, dict] = {}
    for p in smartlab_payments:
        key = p["recordDate"]
        if key not in smartlab_by_date:
            smartlab_by_date[key] = p

    merged: list[dict] = []
    seen_dates: set[str] = set()

    # Process dohod payments (primary)
    for dp in dohod_payments:
        rd = dp["recordDate"]
        seen_dates.add(rd)

        sources: dict = {
            "dohod": {"amount": dp["amount"], "scrapedAt": dohod_scraped}
        }
        statuses = [_dohod_status(dp)]
        amount = dp["amount"]
        year = dp.get("year")
        declared_date = dp.get("declaredDate")

        sp = smartlab_by_date.get(rd)
        if sp:
            sources["smartlab"] = {
                "amount": sp["amount"],
                "scrapedAt": smartlab_scraped,
            }
            statuses.append(sp.get("status", "forecast"))
            if sp.get("year"):
                year = year or sp["year"]

            # Log discrepancy
            if (
                dp["amount"] is not None
                and sp["amount"] is not None
                and dp["amount"] != sp["amount"]
            ):
                print(
                    f"  Amount discrepancy for {ticker} on {rd}: "
                    f"dohod={dp['amount']}, smartlab={sp['amount']}"
                )

        merged.append(
            {
                "recordDate": rd,
                "declaredDate": declared_date,
                "amount": amount,
                "year": year,
                "status": _best_status(statuses),
                "sources": sources,
            }
        )

    # Add smartlab-only payments
    for sp in smartlab_payments:
        rd = sp["recordDate"]
        if rd in seen_dates:
            continue

        merged.append(
            {
                "recordDate": rd,
                "declaredDate": None,
                "amount": sp["amount"],
                "year": sp.get("year"),
                "status": sp.get("status", "forecast"),
                "sources": {
                    "smartlab": {
                        "amount": sp["amount"],
                        "scrapedAt": smartlab_scraped,
                    }
                },
            }
        )

    return merged
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd scripts && python -m pytest stocks/test_stocks.py -v`
Expected: All passed

- [ ] **Step 5: Commit**

```bash
git add scripts/stocks/merge.py scripts/stocks/test_stocks.py
git commit -m "feat: add dividend merge logic for dohod.ru + smartlab.ru"
```

### Task 4.3: Update `scripts/stocks/scrape.py` — dual-source pipeline

**Files:**
- Modify: `scripts/stocks/scrape.py`

- [ ] **Step 1: Update stocks entry point for dual-source**

Replace `scripts/stocks/scrape.py`:

```python
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
```

- [ ] **Step 2: Run all tests**

Run: `cd scripts && python -m pytest stocks/ shared/ -v`
Expected: All passed

- [ ] **Step 3: Commit**

```bash
git add scripts/stocks/scrape.py
git commit -m "feat: dual-source stocks pipeline (dohod.ru + smartlab.ru)

Fetches tickers from both sources, merges payments with
dohod.ru priority for amounts, best-status wins for status."
```

- [ ] **Step 4: Sync prompt for heroincome app**

> Stock dividend data schema has changed in heroincome-data:
>
> **Breaking changes:**
> - `isForecast` (boolean) replaced with `status` (`"paid"` | `"approved"` | `"forecast"`)
> - Top-level `source` field removed
> - New `isin` field (nullable) added at top level
> - New `sources` block per payment: `{ "dohod": { "amount": ..., "scrapedAt": ... }, "smartlab": { ... } }`
>
> **Coverage expansion:** ~127 tickers -> ~222 tickers (smartlab covers more)
>
> **Migration needed:**
> - Replace `isForecast == true` checks with `status == "forecast"` (or `status != "paid"`)
> - Remove any references to the top-level `source` field
> - Update any code that reads `payments[].isForecast` to use `payments[].status`
> - New payments may have `declaredDate: null` (smartlab-only payments don't have this field)

---

## Notes

### Running tests

All tests run from the `scripts/` directory:

```bash
cd scripts && python -m pytest shared/ stocks/ bonds/ funds/ -v
```

### Phase 5 (Crypto) — future

Not planned yet. Will follow the same pattern: `scripts/crypto/` + `data/crypto/` + CI workflow. Spec to be written when requirements are clear.
