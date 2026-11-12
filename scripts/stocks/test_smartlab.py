import json
import os

import pytest

from stocks.smartlab import (
    page_url,
    parse_calendar,
    parse_dividend_page,
    parse_share_pages,
)
from stocks.scrape_smartlab import ScrapeAborted, scrape

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


def fixture(name: str) -> str:
    with open(os.path.join(FIXTURES, name), encoding="utf-8") as f:
        return f.read()


# ---- shares list → dividend pages ----


def test_share_pages_are_the_issuer_codes_with_their_current_tickers():
    # SBERP lives on SBER's page, WTCM on WTCMP's, BISVP on BISV's; the index row has no page.
    assert parse_share_pages(fixture("smartlab_shares.html")) == {
        "BISV": ["BISVP"],
        "SBER": ["SBER", "SBERP"],
        "WTCMP": ["WTCM"],
    }


def test_page_url():
    assert page_url("SBER") == "https://smart-lab.ru/q/SBER/dividend/"


# ---- calendar ----


def test_calendar_tells_board_recommendations_from_forecasts():
    cal = parse_calendar(fixture("smartlab_calendar.html"))
    assert cal[("LKOH", "2026-12-09")] == [(535.0, True)]
    assert cal[("ROSN", "2027-01-12")] == [(9.44, False)]
    # Two dividends with one record date stay two entries.
    assert sorted(cal[("BTBR", "2026-12-21")]) == [(4.21, True), (5.59, True)]


def test_calendar_without_the_table_is_an_error():
    with pytest.raises(ValueError):
        parse_calendar("<html><body><p>Ошибка</p></body></html>")


# ---- dividend page ----


def test_phor_page_rows_and_statuses():
    cal = parse_calendar(fixture("smartlab_calendar.html"))
    rows = parse_dividend_page(fixture("smartlab_phor.html"), cal)
    assert list(rows) == ["PHOR"]
    phor = rows["PHOR"]

    assert phor[0] == {
        "recordDate": "2026-12-13",
        "lastBuyDate": "2026-12-10",
        "period": "2026 н/расп",
        "amount": 309.0,
        "currency": "RUB",
        "status": "recommended",
    }
    canceled = [p for p in phor if p["status"] == "canceled"]
    assert [(p["recordDate"], p["amount"]) for p in canceled] == [("2025-07-05", 201.0)]
    # Both dividends of 11.07.2024 are kept.
    assert sorted(p["amount"] for p in phor if p["recordDate"] == "2024-07-11") == [15.0, 294.0]
    assert {p["status"] for p in phor[1:]} == {"paid", "canceled"}
    # The yearly summary table below is not read as payments.
    assert len(phor) == 8


def test_expected_row_status_comes_from_the_calendar():
    page = fixture("smartlab_phor.html")
    forecast = parse_dividend_page(page, {("PHOR", "2026-12-13"): [(309.0, False)]})
    unknown = parse_dividend_page(page, {})
    assert forecast["PHOR"][0]["status"] == "forecast"
    # Listed as upcoming but absent from the calendar: nobody says whether the board recommended it.
    assert unknown["PHOR"][0]["status"] == "expected"


def test_prefs_on_the_common_share_page_get_their_own_rows():
    rows = parse_dividend_page(fixture("smartlab_sber.html"), {})
    assert sorted(rows) == ["SBER", "SBERP"]
    assert rows["SBER"][0]["recordDate"] == "2026-07-20"
    assert rows["SBER"][0]["amount"] == 37.64
    assert len(rows["SBER"]) == len(rows["SBERP"]) == 3
    assert all(p["status"] == "paid" for p in rows["SBER"] + rows["SBERP"])


def test_page_without_dividend_tables_is_an_error():
    with pytest.raises(ValueError):
        parse_dividend_page("<html><body><h1>404</h1></body></html>", {})


def test_old_ticker_rows_go_to_the_current_ticker_of_the_page():
    rows = parse_dividend_page(fixture("smartlab_abio.html"), {}, current_tickers=["ABIO"])
    assert list(rows) == ["ABIO"]
    assert [(p["recordDate"], p.get("listedAs")) for p in rows["ABIO"]] == [
        ("2026-06-22", None),
        ("2024-06-17", None),
        ("2023-07-19", "ISKJ"),
    ]


def test_old_ticker_rows_keep_their_ticker_when_the_page_has_several():
    rows = parse_dividend_page(fixture("smartlab_abio.html"), {}, current_tickers=["ABIO", "ABIOP"])
    assert sorted(rows) == ["ABIO", "ISKJ"]
    assert "listedAs" not in rows["ISKJ"][0]


def test_the_other_share_class_is_never_folded():
    # /q/shares/ lists only BISVP for the BISV page, but the page also has common BISV rows,
    # with a different amount on the same date — folding them would count both as BISVP.
    rows = parse_dividend_page(fixture("smartlab_bisv.html"), {}, current_tickers=["BISVP"])
    assert sorted(rows) == ["BISV", "BISVP"]
    assert [p["amount"] for p in rows["BISV"]] == [1.06869]
    assert all("listedAs" not in p for p in rows["BISV"] + rows["BISVP"])


def test_a_listed_ticker_is_never_folded():
    rows = parse_dividend_page(
        fixture("smartlab_abio.html"), {}, current_tickers=["ABIO"], listed_tickers={"ABIO", "ISKJ"}
    )
    assert sorted(rows) == ["ABIO", "ISKJ"]


def test_share_that_never_paid_has_no_rows():
    # Only the yearly summary table: «компания ДВМП дивиденды не выплачивала».
    assert parse_dividend_page(fixture("smartlab_fesh.html"), {}) == {}


def test_a_row_listed_as_expected_and_paid_is_kept_once_as_paid():
    head = (
        '<tr class="header_row"><th>Тикер</th><th>дата T-1</th><th>дата отсечки</th>'
        "<th>Период</th><th>дивиденд</th><th>Цена акции</th><th>Див. доходность</th></tr>"
    )
    row = "<tr><td>LKOH</td><td>08.12.2026</td><td>09.12.2026</td><td>3кв 2026</td><td><strong>535</strong>₽</td><td>6015</td><td>8,9%</td></tr>"
    html = (
        '<table class="dividends"><tr><td colspan="8"><h2>Ожидаемые дивиденды Лукойл</h2></td></tr>'
        f"{head}{row}</table>"
        '<table class="dividends"><tr><td colspan="8"><h2>Выплаченные дивиденды Лукойл</h2></td></tr>'
        f"{head}{row}</table>"
    )
    rows = parse_dividend_page(html, {})
    assert [(p["amount"], p["status"]) for p in rows["LKOH"]] == [(535.0, "paid")]


# ---- scrape run ----


class FakeSite:
    """Serves fixtures by URL; `down` is a set of URLs that fail."""

    def __init__(self, down=()):
        self.pages = {
            "https://smart-lab.ru/q/shares/": fixture("smartlab_shares.html"),
            "https://smart-lab.ru/dividends/": fixture("smartlab_calendar.html"),
            page_url("SBER"): fixture("smartlab_sber.html"),
            page_url("BISV"): fixture("smartlab_abio.html"),
            page_url("WTCMP"): "<html><body>Ошибка</body></html>",
        }
        self.down = set(down)
        self.calls = []

    def __call__(self, url):
        self.calls.append(url)
        if url in self.down:
            return None
        return self.pages.get(url)


def read(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def test_scrape_writes_a_file_per_ticker_and_an_index_of_all_files(tmp_path):
    # A file from an earlier run whose page failed this time stays listed.
    (tmp_path / "OLD.json").write_text('{"ticker": "OLD", "payments": []}', encoding="utf-8")

    scrape(FakeSite(), str(tmp_path), pause=0)

    assert read(tmp_path / "SBERP.json")["page"] == page_url("SBER")
    assert read(tmp_path / "SBERP.json")["ticker"] == "SBERP"
    assert len(read(tmp_path / "SBER.json")["payments"]) == 3
    # The page's rows (ABIO, ISKJ) belong to its only current ticker, BISVP here.
    assert [p.get("listedAs") for p in read(tmp_path / "BISVP.json")["payments"]] == ["ABIO", "ABIO", "ISKJ"]
    index = read(tmp_path / "index.json")
    assert index["tickers"] == ["BISVP", "OLD", "SBER", "SBERP"]
    assert index["tickersCount"] == 4


def test_scrape_stops_without_the_calendar_and_writes_nothing(tmp_path):
    site = FakeSite(down={"https://smart-lab.ru/dividends/"})
    with pytest.raises(ScrapeAborted):
        scrape(site, str(tmp_path), pause=0)
    assert os.listdir(tmp_path) == []


def test_scrape_stops_when_the_site_stops_answering(tmp_path):
    site = FakeSite(down={page_url("BISV"), page_url("SBER"), page_url("WTCMP")})
    with pytest.raises(ScrapeAborted):
        scrape(site, str(tmp_path), pause=0, max_failures_in_row=3)
    assert "index.json" not in os.listdir(tmp_path)
