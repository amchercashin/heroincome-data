# heroincome-data

Dividend history database for Russian stocks, scraped from dohod.ru.
Updated automatically on the 1st and 15th of each month via GitHub Actions.

## Data format

`data/dividends/{TICKER}.json` — payment history per ticker.
`data/index.json` — list of all covered tickers.

## Usage

```js
const res = await fetch('https://raw.githubusercontent.com/amchercashin/heroincome-data/main/data/dividends/LKOH.json')
const data = await res.json()
```

## Sources

- [dohod.ru](https://www.dohod.ru/ik/analytics/dividend/) — dividend history for ~80 Russian stocks
