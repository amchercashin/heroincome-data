"""smart-lab.ru: dividend history, upcoming dividends and cancellations of Russian shares.

- /q/shares/ lists every share with the code of its issuer page; a preferred share lives on
  the page of the common one (SBERP on /q/SBER/), so one page can hold rows of several tickers.
- /q/{CODE}/dividend/ has «Ожидаемые дивиденды» (upcoming) and «Выплаченные дивиденды»
  (register closed) tables; a cancelled dividend keeps its row with class `dividend_canceled`.
- /dividends/ is the calendar of upcoming dividends. smart-lab highlights in green
  (class `dividend_approved`) the ones the board of directors recommended; the other rows are
  forecasts. The ticker page does not mark this, so an upcoming row takes its status from the
  calendar.
"""
from __future__ import annotations

import re

from bs4 import BeautifulSoup

from shared.dates import parse_date_dmy

SITE_URL = "https://smart-lab.ru"
SHARES_URL = f"{SITE_URL}/q/shares/"
CALENDAR_URL = f"{SITE_URL}/dividends/"

_PAGE_HREF = re.compile(r"^/q/([A-Za-z0-9-]+)/f/y/$")
_FORUM_HREF = re.compile(r"^/forum/([A-Za-z0-9-]+)$")
_TICKER = re.compile(r"^[A-Z0-9][A-Z0-9-]*$")
_CURRENCIES = {"₽": "RUB", "$": "USD", "€": "EUR", "¥": "CNY"}

# (ticker, record date) → [(amount, recommended by the board)]
Calendar = dict[tuple[str, str], list[tuple[float, bool]]]


def page_url(code: str) -> str:
    return f"{SITE_URL}/q/{code}/dividend/"


def _text(el) -> str:
    return el.get_text(" ", strip=True).replace("\xa0", " ")


def _amount(text: str) -> tuple[float | None, str | None]:
    currency = next((code for sign, code in _CURRENCIES.items() if sign in text), None)
    cleaned = re.sub(r"[^\d,.\-]", "", text).replace(",", ".")
    try:
        return float(cleaned), currency
    except ValueError:
        return None, currency


def parse_share_pages(html: str) -> dict[str, list[str]]:
    """Issuer page code → the current tickers on it, from the /q/shares/ table."""
    soup = BeautifulSoup(html, "html.parser")
    pages: dict[str, set[str]] = {}
    # Only rows of the shares table: the rest of the page links to forums too.
    for row in soup.find_all("tr"):
        ticker_cell = row.find("td", class_="trades-table__ticker")
        if ticker_cell is None:
            continue
        code = None
        for a in row.find_all("a", href=True):
            match = _PAGE_HREF.match(a["href"]) or _FORUM_HREF.match(a["href"])
            if match:
                code = match.group(1)
                if _PAGE_HREF.match(a["href"]):
                    break
        ticker = _text(ticker_cell)
        if code and _TICKER.match(ticker):
            pages.setdefault(code, set()).add(ticker)
    return {code: sorted(tickers) for code, tickers in sorted(pages.items())}


def _column_indexes(header_cells: list[str], wanted: dict[str, str]) -> dict[str, int]:
    found = {}
    for key, title in wanted.items():
        for i, cell in enumerate(header_cells):
            if cell.lower().startswith(title):
                found[key] = i
                break
    missing = set(wanted) - set(found)
    if missing:
        raise ValueError(f"unexpected table header {header_cells}: no {sorted(missing)}")
    return found


def parse_calendar(html: str) -> Calendar:
    """Upcoming dividends from /dividends/ with the board-recommended flag."""
    soup = BeautifulSoup(html, "html.parser")
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if not rows:
            continue
        header = [_text(c) for c in rows[0].find_all(["th", "td"])]
        if not any(h.startswith("Дата закрытия реестра") for h in header):
            continue
        col = _column_indexes(header, {"ticker": "тикер", "amount": "дивиденд", "record": "дата закрытия реестра"})
        calendar: Calendar = {}
        for row in rows[1:]:
            cells = [_text(c) for c in row.find_all("td")]
            if len(cells) <= max(col.values()):
                continue
            record = parse_date_dmy(cells[col["record"]])
            amount, _ = _amount(cells[col["amount"]])
            if not record or amount is None or not _TICKER.match(cells[col["ticker"]]):
                continue
            approved = "dividend_approved" in (row.get("class") or [])
            calendar.setdefault((cells[col["ticker"]], record), []).append((amount, approved))
        return calendar
    raise ValueError("no dividend calendar table on the page")


def _upcoming_status(calendar: Calendar, ticker: str, record: str, amount: float) -> str:
    for cal_amount, approved in calendar.get((ticker, record), []):
        if abs(cal_amount - amount) < 1e-9:
            return "recommended" if approved else "forecast"
    return "expected"


def parse_dividend_page(
    html: str, calendar: Calendar, current_tickers: list[str] | None = None
) -> dict[str, list[dict]]:
    """Rows of a /q/{CODE}/dividend/ page by ticker, newest first as on the page.

    Status: `paid` — the register has closed (the «Выплаченные» table); `canceled`;
    upcoming rows — `recommended` (by the board), `forecast`, or `expected` when the calendar
    does not list them.

    A renamed company keeps its old rows under the old ticker (ABIO's page lists ISKJ). When the
    page has one current ticker, such rows go to it with `listedAs` = the ticker on the page;
    with several (common + preferred) it is unknown whose they are, so they keep their own.
    """
    current = set(current_tickers or [])
    soup = BeautifulSoup(html, "html.parser")
    all_tables = soup.select("table.dividends")
    # Every share page has at least the yearly summary table; none at all means another page
    # (404, an error page) or a new layout.
    if not all_tables:
        raise ValueError("no dividend tables on the page")
    tables: list[tuple[bool, object]] = []
    for table in all_tables:
        title = table.find("h2")
        title_text = _text(title) if title else ""
        if title_text.startswith("Ожидаемые дивиденды"):
            tables.append((True, table))
        elif title_text.startswith("Выплаченные дивиденды"):
            tables.append((False, table))
    # Only the summary table: the company has never paid (smart-lab: «дивиденды не выплачивала»).

    by_ticker: dict[str, list[dict]] = {}
    # Paid rows first, so a row the page lists as upcoming and paid at once stays paid.
    for upcoming, table in sorted(tables, key=lambda t: t[0]):
        header_row = table.find("tr", class_="header_row")
        if header_row is None:
            raise ValueError("dividend table has no header row")
        col = _column_indexes(
            [_text(c) for c in header_row.find_all(["th", "td"])],
            {"ticker": "тикер", "buy": "дата t-1", "record": "дата отсечки", "period": "период", "amount": "дивиденд"},
        )
        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) <= max(col.values()):
                continue
            texts = [_text(c) for c in cells]
            ticker = texts[col["ticker"]]
            record = parse_date_dmy(texts[col["record"]])
            amount, currency = _amount(texts[col["amount"]])
            if not _TICKER.match(ticker) or not record or amount is None:
                continue

            if "dividend_canceled" in (row.get("class") or []):
                status = "canceled"
            elif upcoming:
                status = _upcoming_status(calendar, ticker, record, amount)
            else:
                status = "paid"

            owner, listed_as = ticker, None
            if len(current) == 1 and ticker not in current:
                owner, listed_as = next(iter(current)), ticker
            payments = by_ticker.setdefault(owner, [])
            if upcoming and any(p["recordDate"] == record and p["amount"] == amount for p in payments):
                continue
            payment = {
                "recordDate": record,
                "lastBuyDate": parse_date_dmy(texts[col["buy"]]),
                "period": texts[col["period"]] or None,
                "amount": amount,
                "currency": currency,
                "status": status,
            }
            if listed_as:
                payment["listedAs"] = listed_as
            payments.append(payment)

    for payments in by_ticker.values():
        payments.sort(key=lambda p: p["recordDate"], reverse=True)
    return by_ticker
