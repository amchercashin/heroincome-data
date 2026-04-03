from bonds.moex_iss import parse_securities_listing, parse_bondization

LISTING_RESPONSE = {
    "securities": {
        "columns": ["SECID", "ISIN", "SHORTNAME", "FACEVALUE", "FACEUNIT",
                     "MATDATE", "COUPONVALUE", "NEXTCOUPON", "COUPONPERIOD",
                     "SECTYPE"],
        "data": [
            ["SU26238RMFS4", "RU000A1038V6", "ОФЗ 26238", 1000.0, "SUR",
             "2041-05-28", 33.91, "2026-06-04", 182, "3"],
            ["RU000A106T36", "RU000A106T36", "Сбер Б 002Р-20R", 1000.0, "SUR",
             "2027-04-15", 0.0, "2026-07-17", 91, "6"],
        ],
    }
}

BONDIZATION_RESPONSE = {
    "coupons": {
        "columns": ["isin", "name", "issuevalue", "coupondate", "recorddate",
                     "startdate", "initialfacevalue", "facevalue", "faceunit",
                     "value", "valueprc", "value_rub", "secid", "primary_boardid"],
        "data": [
            ["RU000A1038V6", "ОФЗ 26238", 500000000000, "2019-11-20",
             "2019-11-19", "2019-05-22", 1000.0, 1000.0, "SUR",
             33.91, 6.9, 33.91, "SU26238RMFS4", "TQOB"],
            ["RU000A1038V6", "ОФЗ 26238", 500000000000, "2020-05-20",
             "2020-05-19", "2019-11-20", 1000.0, 1000.0, "SUR",
             33.91, 6.9, 33.91, "SU26238RMFS4", "TQOB"],
        ],
    },
    "amortizations": {
        "columns": ["isin", "name", "issuevalue", "amortdate", "facevalue",
                     "initialfacevalue", "faceunit", "value", "valueprc",
                     "value_rub", "data_source", "secid", "primary_boardid"],
        "data": [
            ["RU000A1038V6", "ОФЗ 26238", 500000000000, "2041-05-28",
             1000.0, 1000.0, "SUR", 1000.0, 100.0, 1000.0,
             "maturity", "SU26238RMFS4", "TQOB"],
        ],
    },
    "offers": {
        "columns": ["isin", "name", "issuevalue", "offerdate", "offerdatestart",
                     "offerdateend", "facevalue", "faceunit", "price", "value",
                     "agent", "offertype", "secid", "primary_boardid"],
        "data": [],
    },
}


def test_parse_securities_listing_extracts_bonds():
    bonds = parse_securities_listing(LISTING_RESPONSE)
    assert len(bonds) == 2
    assert bonds[0]["secid"] == "SU26238RMFS4"
    assert bonds[0]["isin"] == "RU000A1038V6"
    assert bonds[0]["name"] == "ОФЗ 26238"
    assert bonds[0]["faceValue"] == 1000.0
    assert bonds[0]["currency"] == "SUR"
    assert bonds[0]["matDate"] == "2041-05-28"


def test_parse_securities_listing_handles_empty():
    empty = {"securities": {"columns": LISTING_RESPONSE["securities"]["columns"], "data": []}}
    assert parse_securities_listing(empty) == []


def test_parse_bondization_extracts_coupons():
    result = parse_bondization(BONDIZATION_RESPONSE, "SU26238RMFS4")
    assert len(result["coupons"]) == 2
    c = result["coupons"][0]
    assert c["couponDate"] == "2019-11-20"
    assert c["recordDate"] == "2019-11-19"
    assert c["value"] == 33.91
    assert c["valuePrc"] == 6.9
    assert c["startDate"] == "2019-05-22"


def test_parse_bondization_extracts_amortizations():
    result = parse_bondization(BONDIZATION_RESPONSE, "SU26238RMFS4")
    assert len(result["amortizations"]) == 1
    a = result["amortizations"][0]
    assert a["amortDate"] == "2041-05-28"
    assert a["value"] == 1000.0
    assert a["valuePrc"] == 100.0
    assert a["type"] == "maturity"


def test_parse_bondization_handles_empty_offers():
    result = parse_bondization(BONDIZATION_RESPONSE, "SU26238RMFS4")
    assert result["offers"] == []


def test_parse_bondization_handles_null_coupon_value():
    """Floater bonds have null coupon values for future dates."""
    data = {
        "coupons": {
            "columns": BONDIZATION_RESPONSE["coupons"]["columns"],
            "data": [
                ["RU000A106T36", "Сбер Б", 10000000000, "2026-07-17",
                 "2026-07-16", "2026-04-17", 1000.0, 1000.0, "SUR",
                 None, None, None, "RU000A106T36", "TQCB"],
            ],
        },
        "amortizations": {"columns": [], "data": []},
        "offers": {"columns": [], "data": []},
    }
    result = parse_bondization(data, "RU000A106T36")
    assert result["coupons"][0]["value"] is None
    assert result["coupons"][0]["valuePrc"] is None
