from __future__ import annotations

from datetime import datetime, timezone

ISS_BASE = "https://iss.moex.com/iss"
LISTING_URL = f"{ISS_BASE}/engines/stock/markets/bonds/securities.json"
BONDIZATION_URL = f"{ISS_BASE}/securities/{{secid}}/bondization.json?limit=200"
SECURITY_URL = f"{ISS_BASE}/securities/{{secid}}.json"


def _columns_data_to_dicts(block: dict) -> list[dict]:
    """Convert MOEX ISS {columns: [...], data: [[...], ...]} to list of dicts."""
    columns = block.get("columns", [])
    return [dict(zip(columns, row)) for row in block.get("data", [])]


def parse_securities_listing(response_json: dict) -> list[dict]:
    """Parse /engines/stock/markets/bonds/securities.json response."""
    rows = _columns_data_to_dicts(response_json.get("securities", {}))
    bonds = []
    for row in rows:
        bonds.append(
            {
                "secid": row["SECID"],
                "isin": row.get("ISIN"),
                "name": row.get("SHORTNAME"),
                "faceValue": row.get("FACEVALUE"),
                "currency": row.get("FACEUNIT"),
                "matDate": row.get("MATDATE"),
            }
        )
    return bonds


def parse_bondization(response_json: dict, secid: str) -> dict:
    """Parse /securities/{secid}/bondization.json response."""
    coupon_rows = _columns_data_to_dicts(response_json.get("coupons", {}))
    amort_rows = _columns_data_to_dicts(response_json.get("amortizations", {}))
    offer_rows = _columns_data_to_dicts(response_json.get("offers", {}))

    coupons = [
        {
            "couponDate": r.get("coupondate"),
            "recordDate": r.get("recorddate"),
            "value": r.get("value"),
            "valuePrc": r.get("valueprc"),
            "startDate": r.get("startdate"),
        }
        for r in coupon_rows
    ]

    amortizations = [
        {
            "amortDate": r.get("amortdate"),
            "value": r.get("value"),
            "valuePrc": r.get("valueprc"),
            "type": r.get("data_source", "amortization"),
        }
        for r in amort_rows
    ]

    offers = [
        {
            "offerDate": r.get("offerdate"),
            "offerType": r.get("offertype"),
            "value": r.get("value"),
        }
        for r in offer_rows
    ]

    return {
        "secid": secid,
        "scrapedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "coupons": coupons,
        "amortizations": amortizations,
        "offers": offers,
    }
