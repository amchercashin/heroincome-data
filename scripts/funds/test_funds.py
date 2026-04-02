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
