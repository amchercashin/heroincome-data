from __future__ import annotations

STATUS_PRIORITY = {"paid": 0, "approved": 1, "forecast": 2}


def _dohod_status(payment: dict) -> str:
    """Convert dohod.ru isForecast field to status string."""
    return "forecast" if payment.get("isForecast") else "paid"


def _best_status(statuses: list[str]) -> str:
    """Return the highest-priority status."""
    return min(statuses, key=lambda s: STATUS_PRIORITY.get(s, 99))


def merge_payments(
    dohod_data: dict | None,
    smartlab_data: dict | None,
    ticker: str,
) -> list[dict]:
    """Merge payments from dohod.ru and smartlab.ru.

    Priority: dohod.ru for amounts. Best (most certain) status wins.
    Matching by ticker + recordDate.
    """
    dohod_payments = (dohod_data or {}).get("payments", [])
    smartlab_payments = (smartlab_data or {}).get("payments", [])
    dohod_scraped = (dohod_data or {}).get("scrapedAt")
    smartlab_scraped = (smartlab_data or {}).get("scrapedAt")

    smartlab_by_date: dict[str, dict] = {}
    for p in smartlab_payments:
        key = p["recordDate"]
        if key not in smartlab_by_date:
            smartlab_by_date[key] = p

    merged: list[dict] = []
    seen_dates: set[str] = set()

    for dp in dohod_payments:
        rd = dp["recordDate"]
        seen_dates.add(rd)

        sources: dict = {
            "dohod": {"amount": dp["amount"], "scrapedAt": dohod_scraped}
        }
        statuses = [_dohod_status(dp)]
        amount = dp["amount"]
        year = dp.get("year")
        declared_date = dp.get("declaredDate")

        sp = smartlab_by_date.get(rd)
        if sp:
            sources["smartlab"] = {
                "amount": sp["amount"],
                "scrapedAt": smartlab_scraped,
            }
            statuses.append(sp.get("status", "forecast"))
            if sp.get("year"):
                year = year or sp["year"]

            if (
                dp["amount"] is not None
                and sp["amount"] is not None
                and dp["amount"] != sp["amount"]
            ):
                print(
                    f"  Amount discrepancy for {ticker} on {rd}: "
                    f"dohod={dp['amount']}, smartlab={sp['amount']}"
                )

        merged.append(
            {
                "recordDate": rd,
                "declaredDate": declared_date,
                "amount": amount,
                "year": year,
                "status": _best_status(statuses),
                "sources": sources,
            }
        )

    for sp in smartlab_payments:
        rd = sp["recordDate"]
        if rd in seen_dates:
            continue

        merged.append(
            {
                "recordDate": rd,
                "declaredDate": None,
                "amount": sp["amount"],
                "year": sp.get("year"),
                "status": sp.get("status", "forecast"),
                "sources": {
                    "smartlab": {
                        "amount": sp["amount"],
                        "scrapedAt": smartlab_scraped,
                    }
                },
            }
        )

    return merged
