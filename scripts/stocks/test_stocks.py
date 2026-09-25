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
