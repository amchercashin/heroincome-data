from __future__ import annotations

import csv
import io
import re
from datetime import datetime, timezone

from shared.dates import parse_date_dmy

MONTHS_RU = {
    "янв": 1, "фев": 2, "мар": 3, "апр": 4,
    "май": 5, "мая": 5, "июн": 6, "июл": 7,
    "авг": 8, "сен": 9, "окт": 10, "ноя": 11, "дек": 12,
}

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
    cleaned = re.sub(r"\(.*?\)", "", text).strip()
    return _parse_russian_float(cleaned)


def _parse_parus_date(text: str) -> str | None:
    """Parse Parus date formats:
    - '14 мар 2026' -> '2026-03-14'
    - '14.03.2026'  -> '2026-03-14'
    - '13.07'       -> None (no year = skip)
    """
    text = text.strip()
    if not text:
        return None

    # Try DD.MM.YYYY first
    result = parse_date_dmy(text)
    if result:
        return result

    # Try "DD мес YYYY" format
    match = re.match(r"(\d{1,2})\s+(\w{3,4})\s+(\d{4})", text)
    if match:
        day, month_str, year = match.groups()
        month = MONTHS_RU.get(month_str.lower()[:3])
        if month:
            return f"{year}-{month:02d}-{int(day):02d}"

    return None


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

        payment_date = _parse_parus_date(payment_clean)
        record_date = _parse_parus_date(record_clean)

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
