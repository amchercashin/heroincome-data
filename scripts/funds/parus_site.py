"""Parus funds from their own site, parus.properties.

Every fund has a page under /funds/<slug>/ with its ISIN and a "Выплаты дохода
инвесторам" table holding the full payout history. The fund list is read from
the site, so a newly listed fund is picked up without code changes.
"""
from __future__ import annotations

import re
from datetime import date, datetime, timezone

from bs4 import BeautifulSoup

from funds.parus import _clean_planned, _parse_parus_date, _parse_russian_float

SITE_URL = "https://parus.properties"
FUNDS_LIST_URL = f"{SITE_URL}/funds/"

_FUND_HREF = re.compile(r"^(?:https?://(?:www\.)?parus\.properties)?/funds/([a-z0-9][a-z0-9_-]*)/?$")
_ISIN = re.compile(r"\b(RU[0-9A-Z]{9}[0-9])\b")
_DAY_MONTH = re.compile(r"^(\d{1,2})\.(\d{1,2})$")


def discover_fund_urls(html: str) -> list[str]:
    """Fund page URLs linked from a parus.properties page, sorted and unique."""
    soup = BeautifulSoup(html, "html.parser")
    slugs = set()
    for a in soup.find_all("a", href=True):
        match = _FUND_HREF.match(a["href"].strip())
        if match:
            slugs.add(match.group(1))
    return [f"{SITE_URL}/funds/{slug}/" for slug in sorted(slugs)]


def pick_secid(rows: list[list[str]], isin: str) -> str | None:
    """From MOEX ISS search rows [secid, isin], the secid of the given ISIN."""
    for secid, row_isin in rows:
        if row_isin == isin:
            return secid
    return None


def _text(el) -> str:
    return el.get_text(" ", strip=True).replace("\xa0", " ")


def _find_isin(soup: BeautifulSoup) -> str:
    for el in soup.select("[data-copy-value]"):
        value = el["data-copy-value"].strip()
        if _ISIN.fullmatch(value) and "ISIN" in _text(el):
            return value
    found = set(_ISIN.findall(soup.get_text(" ")))
    if len(found) == 1:
        return found.pop()
    raise ValueError(f"expected one ISIN on the page, found {sorted(found) or 'none'}")


def _column_index(headers: list[str], *needles: str) -> int | None:
    for i, header in enumerate(headers):
        low = header.lower()
        if all(n in low for n in needles):
            return i
    return None


def _payment_date(raw: str, record_date: str | None) -> str | None:
    """'12.11.2026' / '14 мар 2026' as is; a planned '~10.12' takes its year from the record date."""
    parsed = _parse_parus_date(raw)
    if parsed or not record_date:
        return parsed
    match = _DAY_MONTH.match(raw.strip())
    if not match:
        return None
    day, month = int(match.group(1)), int(match.group(2))
    rec_year, rec_month = int(record_date[:4]), int(record_date[5:7])
    year = rec_year + 1 if month < rec_month else rec_year
    try:
        return date(year, month, day).isoformat()
    except ValueError:
        return None


def parse_fund_page(html: str, url: str, today: date) -> dict | None:
    """Parse a fund page. None when the fund has no payouts table (e.g. ПАРУС-МАКС).

    Planned rows are published only once their register has closed: consumers
    read every row as a real payment, and the amount is settled by then.
    """
    soup = BeautifulSoup(html, "html.parser")
    isin = _find_isin(soup)

    table = soup.select_one("div.table-income")
    if table is None:
        return None
    head = table.select_one(".table-income__head")
    if head is None:
        raise ValueError("payouts table has no header")
    headers = [_text(c) for c in head.select(".table-income__column")]

    col_payment = _column_index(headers, "дата выплаты")
    col_record = _column_index(headers, "реестр")
    col_amount = _column_index(headers, "до ндфл")
    col_price = _column_index(headers, "стоимость пая")
    col_yield = _column_index(headers, "доходность", "цены ук")
    if col_payment is None or col_record is None or col_amount is None:
        raise ValueError(f"unexpected payouts table header: {headers}")

    distributions: list[dict] = []
    for row in table.select(".table-income__string"):
        cells = [_text(c) for c in row.select(".table-income__column")]

        def cell(i: int | None) -> str:
            return cells[i] if i is not None and i < len(cells) else ""

        payment_raw, is_planned = _clean_planned(cell(col_payment))
        record_raw, _ = _clean_planned(cell(col_record))
        record_date = _parse_parus_date(record_raw)
        payment_date = _payment_date(payment_raw, record_date)
        amount = _parse_russian_float(cell(col_amount))

        effective = record_date or payment_date
        if not effective or amount is None:
            continue
        if is_planned and date.fromisoformat(effective) > today:
            continue

        distributions.append(
            {
                "paymentDate": payment_date,
                "recordDate": record_date,
                "unitPrice": _parse_russian_float(cell(col_price)),
                "amountBeforeTax": amount,
                "amountAfterTax": None,
                "yieldPrc": _parse_russian_float(cell(col_yield)),
                "status": "planned" if is_planned else "paid",
            }
        )

    title = soup.select_one(".intro-fund__banner-title")
    return {
        "isin": isin,
        "ticker": None,
        "name": _text(title) if title else isin,
        "managementCompany": "Parus",
        "source": url,
        "scrapedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "distributions": distributions,
    }
