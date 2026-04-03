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


