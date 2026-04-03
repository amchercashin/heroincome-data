from funds.parus import parse_parus_csv, PARUS_FUNDS

# Real format from Google Sheets (Russian text month dates)
CSV_DATA = """Дата выплаты ежемесячного дохода,Закрытие реестра УК,"Стоимость пая, ₽","Доход на 1 пай до НДФЛ, ₽","Доход после НДФЛ 13% на 1 пай, ₽"
14 мар 2026,28 фев 2026,8 100,"72,45 (10,7%)","63,03"
14 фев 2026,31 янв 2026,8 050,"71,10 (10,6%)","61,86"
~13.04 (план),31 мар 2026,8 100,"72,45 (10,7%)","63,03"
"""

# DD.MM.YYYY format also supported
CSV_DATA_DMY = """Дата выплаты,Закрытие реестра,Стоимость пая,Доход до НДФЛ,Доход после НДФЛ
15.03.2026,10.03.2026,"1 250,00","9,52 (0,76%)","8,28"
"""

CSV_EMPTY = """Дата выплаты ежемесячного дохода,Закрытие реестра УК,"Стоимость пая, ₽","Доход на 1 пай до НДФЛ, ₽","Доход после НДФЛ 13% на 1 пай, ₽"
"""


def test_parse_parus_csv_extracts_distributions():
    result = parse_parus_csv(CSV_DATA, "ПАРУС-ОЗН", isin="RU000A1022Z1", ticker="PLZ5")
    assert len(result["distributions"]) == 2  # planned without year is skipped
    assert result["name"] == "ПАРУС-ОЗН"
    assert result["isin"] == "RU000A1022Z1"
    assert result["ticker"] == "PLZ5"
    assert result["managementCompany"] == "Parus"


def test_parse_parus_csv_parses_paid_row():
    result = parse_parus_csv(CSV_DATA, "ПАРУС-ОЗН", isin="RU000A1022Z1")
    paid = [d for d in result["distributions"] if d["status"] == "paid"]
    assert len(paid) == 2
    d = paid[0]
    assert d["paymentDate"] == "2026-03-14"
    assert d["recordDate"] == "2026-02-28"
    assert d["unitPrice"] == 8100.0
    assert d["amountBeforeTax"] == 72.45
    assert d["amountAfterTax"] == 63.03
    assert d["yieldPrc"] == 10.7


def test_parse_parus_csv_parses_dmy_format():
    result = parse_parus_csv(CSV_DATA_DMY, "TEST", isin="TEST")
    assert len(result["distributions"]) == 1
    d = result["distributions"][0]
    assert d["paymentDate"] == "2026-03-15"
    assert d["recordDate"] == "2026-03-10"


def test_parse_parus_csv_skips_planned_without_year():
    result = parse_parus_csv(CSV_DATA, "TEST", isin="TEST")
    # "~13.04 (план)" has no year -> payment_date is None -> skipped
    planned = [d for d in result["distributions"] if d["status"] == "planned"]
    assert len(planned) == 0


def test_parse_parus_csv_handles_empty():
    result = parse_parus_csv(CSV_EMPTY, "ПАРУС-ОЗН", isin="RU000A1022Z1")
    assert result["distributions"] == []


def test_parus_funds_has_8_entries():
    assert len(PARUS_FUNDS) == 8


def test_parse_parus_csv_yield_extraction():
    result = parse_parus_csv(CSV_DATA, "TEST", isin="TEST")
    d = result["distributions"][0]
    assert d["yieldPrc"] == 10.7
