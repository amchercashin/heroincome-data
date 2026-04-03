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
