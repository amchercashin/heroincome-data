# heroincome-data

Pure data repository — stores raw dividend/distribution data from external sources.
No business logic, no merge, no prioritization. Consuming apps decide how to use data.

## Commands

```bash
# Tests (run from scripts/)
cd scripts && python3 -m pytest shared/ stocks/ funds/ -v

# Scrape stocks (dohod.ru)
cd scripts && python3 -m stocks.scrape

# Scrape fund distributions (parus.properties, Google Sheets fallback)
cd scripts && python3 -m funds.scrape
```

## Architecture

```
scripts/
  shared/          # network (retry), io (json save), dates (parsing)
  stocks/
    dohod.py       # parser for dohod.ru HTML
    scrape.py      # entry point
  funds/
    parus_site.py  # parus.properties: fund list + payouts table per fund page
    parus.py       # Parus Google Sheets CSV (fallback for the 8 funds known before)
    scrape.py      # entry point — site first, sheets for what the site missed, MOEX secid → aliases
data/
  stocks/dohod/    # raw dohod.ru data per ticker
  funds/distributions/  # raw Parus fund data per fund, file name = ISIN
```

Each source saves to its own directory with its own index.json.
smartlab.ru was removed (unused by the app, blocked from GitHub Actions IPs).

## CI

- **Stocks**: 1st & 15th of month, 09:00 UTC (`update-dividends.yml`)
- **Funds**: 5th of month, 09:00 UTC (`update-funds.yml`)

## Gotchas

- Google Sheets CSV needs `resp.content.decode('utf-8')`, not `resp.text` (wrong default encoding)
- Parus dates use Russian text months ("14 мар 2026"), not DD.MM.YYYY; sheets have typos ("фер") — add aliases to `MONTHS_RU`; planned payment dates have no year ("~10.12 (план)") — it is taken from the record date
- Funds: files and index keys are ISINs; a MOEX secid that differs (XTRIUMF) goes to `aliases` in index.json. The index lists every file on disk, so a fund that fails one run keeps its data
- Planned payouts are published only after their register closes — the app reads every row as a real payment
- ПАРУС-МАКС has no payouts table (reinvests) and is skipped; ПАРУС-Сити / ПАРУС-ДМД trade on MOEX but have no page on parus.properties yet
- All parsers run from `scripts/` directory (imports are relative to it)
- After data structure changes, always propose a sync prompt for the heroincome app
