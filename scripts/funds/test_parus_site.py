import os
from datetime import date

import pytest

from funds.parus_site import discover_fund_urls, parse_fund_page, pick_secid

FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "parus_fund_page.html")
PAGE_URL = "https://parus.properties/funds/nino/"


def _page() -> str:
    with open(FIXTURE, encoding="utf-8") as f:
        return f.read()


def test_discover_fund_urls_finds_every_fund_page_once():
    html = """
      <a href="/funds/">Все фонды</a>
      <a href="/funds/nino/">НиНо</a> <a href="/funds/nino/">НиНо снова</a>
      <a href="/funds/mv">Место встречи</a>
      <a href="https://parus.properties/funds/ozon/">ОЗН</a>
      <a href="/funds/nino/#payouts">якорь</a>
      <a href="/blog/">Блог</a>
    """
    assert discover_fund_urls(html) == [
        "https://parus.properties/funds/mv/",
        "https://parus.properties/funds/nino/",
        "https://parus.properties/funds/ozon/",
    ]


def test_parse_fund_page_reads_isin_and_name():
    data = parse_fund_page(_page(), PAGE_URL, today=date(2026, 11, 11))
    assert data["isin"] == "RU000A10BBS6"
    assert data["name"] == "ПАРУС-НиНо"
    assert data["managementCompany"] == "Parus"
    assert data["source"] == PAGE_URL


def test_parse_fund_page_keeps_paid_rows():
    data = parse_fund_page(_page(), PAGE_URL, today=date(2026, 11, 11))
    paid = data["distributions"][0]
    assert paid == {
        "paymentDate": "2026-11-12",
        "recordDate": "2026-10-30",
        "unitPrice": 1300.0,
        "amountBeforeTax": 9.38,
        "amountAfterTax": None,
        "yieldPrc": 12.5,
        "status": "paid",
    }
    assert data["distributions"][-1]["recordDate"] == "2026-04-30"


def test_parse_fund_page_drops_planned_rows_with_future_record_date():
    # Consumers treat every row as a real payment, so a plan whose register is
    # still open must not be published yet.
    data = parse_fund_page(_page(), PAGE_URL, today=date(2026, 11, 11))
    assert [d["recordDate"] for d in data["distributions"]] == ["2026-10-30", "2026-04-30"]


def test_parse_fund_page_keeps_planned_row_once_register_closed():
    # "~10.12 (план)" has no year: it is taken from the record date (30 ноя 2026).
    data = parse_fund_page(_page(), PAGE_URL, today=date(2026, 12, 1))
    planned = data["distributions"][0]
    assert planned["recordDate"] == "2026-11-30"
    assert planned["paymentDate"] == "2026-12-10"
    assert planned["status"] == "planned"


def test_parse_fund_page_rolls_payment_year_over_new_year():
    data = parse_fund_page(_page(), PAGE_URL, today=date(2027, 1, 5))
    planned = data["distributions"][0]
    assert planned["recordDate"] == "2026-12-30"
    assert planned["paymentDate"] == "2027-01-20"


def test_parse_fund_page_without_payouts_table_returns_none():
    # ПАРУС-МАКС reinvests income: its page has no payouts table.
    html = '<span class="fond-summary__item--copy" data-copy-value="RU000A108VR7">ISIN</span>'
    assert parse_fund_page(html, "https://parus.properties/funds/max/", today=date(2026, 11, 11)) is None


def test_parse_fund_page_without_isin_raises():
    html = _page().replace("RU000A10BBS6", "")
    with pytest.raises(ValueError):
        parse_fund_page(html, PAGE_URL, today=date(2026, 11, 11))


def test_parse_fund_page_finds_columns_by_header_not_position():
    html = _page().replace(
        '<div class="table-income__column">Биржевая цена, ₽</div>', ""
    ).replace('<div class="table-income__column">736</div>', "")
    data = parse_fund_page(html, PAGE_URL, today=date(2026, 11, 11))
    assert data["distributions"][0]["amountBeforeTax"] == 9.38


def test_pick_secid_prefers_exact_isin_match():
    rows = [["XTRIUMF", "RU000A108BZ2"], ["RU000A108BZ2X", "RU000A108BZ3"]]
    assert pick_secid(rows, "RU000A108BZ2") == "XTRIUMF"
    assert pick_secid(rows, "RU000A000000") is None
