# heroincome-data

Raw dividend and fund distribution data for the [Рантье](https://github.com/amchercashin/rantie) app (formerly heroincome).
Pure data: no merging or prioritization — consuming apps decide how to use it.

## How the data flows

```
GitHub Actions, every Monday                     Рантье (in the user's browser)
────────────────────────────                     ──────────────────────────────
dohod.ru ─────────► data/stocks/dohod/ ─┐
                                        ├─ raw.githubusercontent.com ─► read on every sync
parus.properties ─► data/funds/ ────────┘

MOEX ISS ──────────────────── read by the app directly ───────────────► prices, bond coupons,
                                                                        exchange rates
```

- A browser cannot scrape other sites (CORS, HTML), so the scrapers run here on a schedule and
  publish plain JSON that the app fetches as static files.
- Since autumn 2026 MOEX ISS serves dividends to paying subscribers only, so these files are the
  app's only source of stock dividends (dohod.ru) and fund distributions (Parus). The app prefers
  them per security (dohod.ru for stocks, Parus for funds) and keeps one source per security.
- A run commits only when the data changed. Every run rewrites `scrapedAt` / `updatedAt`; a run
  where nothing else changed commits nothing, so `scrapedAt` is when the file was last written
  with new data, and the Actions log shows every check.

## Sources and schedule

Scheduled runs can start later than the cron time when GitHub Actions is busy; both workflows can also be run by hand (`gh workflow run update-dividends.yml` / `update-funds.yml`).

| Source | What | Updated (GitHub Actions) |
|---|---|---|
| [dohod.ru](https://www.dohod.ru/ik/analytics/dividend/) | Dividend history and forecasts, ~120 Russian stocks | Every Monday, 05:17 UTC |
| [parus.properties](https://parus.properties/funds/) | Monthly distributions of every Parus closed-end real estate fund with payouts (the fund list is read from the site; Parus Google Sheets are a fallback for the funds known before) | Every Monday, 05:47 UTC |

## Data format

### Stocks — `data/stocks/dohod/`

`index.json` — `{ updatedAt, tickersCount, tickers: [...] }`

`{TICKER}.json`:

```json
{
  "ticker": "LKOH",
  "scrapedAt": "2026-09-15T14:17:33Z",
  "payments": [
    { "recordDate": "2026-05-04", "declaredDate": "2026-03-20", "amount": 278.0, "year": 2026, "isForecast": false }
  ]
}
```

A forecast with `"amount": 0` means dohod.ru expects no dividend.

### Funds — `data/funds/`

`index.json` — `{ updatedAt, fundsCount, funds: [...], aliases: { SECID: ISIN } }`. Each key is the fund's ISIN. `aliases` maps a MOEX code that differs from the ISIN (e.g. `XTRIUMF`) to the fund's key.

`distributions/{KEY}.json`:

```json
{
  "isin": "RU000A1022Z1",
  "ticker": null,
  "name": "ПАРУС-ОЗН",
  "managementCompany": "Parus",
  "source": "https://parus.properties/funds/ozon/",
  "scrapedAt": "2026-09-05T12:26:13Z",
  "distributions": [
    { "paymentDate": "2026-08-13", "recordDate": "2026-07-31", "unitPrice": 8100.0,
      "amountBeforeTax": 72.45, "amountAfterTax": null, "yieldPrc": 10.7, "status": "paid" }
  ]
}
```

`ticker` is the MOEX code when it differs from the ISIN, otherwise `null`. `amountAfterTax` is filled only by the Google Sheets fallback. A planned payout (`"status": "planned"`) is published only once its register has closed; until then it is left out. `recordDate` can be `null` if the source has no parsable date.

## Usage

```js
const BASE = 'https://raw.githubusercontent.com/amchercashin/heroincome-data/main/data'
const lkoh = await (await fetch(`${BASE}/stocks/dohod/LKOH.json`)).json()
const ozn = await (await fetch(`${BASE}/funds/distributions/RU000A1022Z1.json`)).json()
```

## Development

```bash
pip install -r requirements.txt
cd scripts
python3 -m pytest shared/ stocks/ funds/ -v   # tests
python3 -m stocks.scrape                      # dohod.ru
python3 -m funds.scrape                       # Parus funds (parus.properties)
```
