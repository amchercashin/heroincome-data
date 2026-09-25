# heroincome-data

Raw dividend and fund distribution data for the [heroincome](https://github.com/amchercashin/heroincome) app.
Pure data: no merging or prioritization — consuming apps decide how to use it.

## Sources and schedule

| Source | What | Updated (GitHub Actions) |
|---|---|---|
| [dohod.ru](https://www.dohod.ru/ik/analytics/dividend/) | Dividend history and forecasts, ~120 Russian stocks | 1st and 15th of each month |
| Parus Google Sheets | Monthly distributions of 8 Parus closed-end real estate funds | 5th of each month |

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

`index.json` — `{ updatedAt, fundsCount, funds: [...] }`. Each key is the fund's exchange ticker when it has one, otherwise its ISIN.

`distributions/{KEY}.json`:

```json
{
  "isin": "RU000A1022Z1",
  "ticker": "PLZ5",
  "name": "ПАРУС-ОЗН",
  "managementCompany": "Parus",
  "scrapedAt": "2026-09-05T12:26:13Z",
  "distributions": [
    { "paymentDate": "2026-08-13", "recordDate": "2026-07-31", "unitPrice": 8100.0,
      "amountBeforeTax": 72.45, "amountAfterTax": 63.03, "yieldPrc": 10.7, "status": "paid" }
  ]
}
```

`recordDate` can be `null` if the source sheet has no parsable date.

## Usage

```js
const BASE = 'https://raw.githubusercontent.com/amchercashin/heroincome-data/main/data'
const lkoh = await (await fetch(`${BASE}/stocks/dohod/LKOH.json`)).json()
const plz5 = await (await fetch(`${BASE}/funds/distributions/PLZ5.json`)).json()
```

## Development

```bash
pip install -r requirements.txt
cd scripts
python3 -m pytest shared/ stocks/ funds/ -v   # tests
python3 -m stocks.scrape                      # dohod.ru
python3 -m funds.scrape                       # Parus funds
```
