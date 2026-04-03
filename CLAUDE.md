# heroincome-data

Pure data repository — stores raw dividend/distribution data from external sources.
No business logic, no merge, no prioritization. Consuming apps decide how to use data.

## Commands

```bash
# Tests (run from scripts/)
cd scripts && python3 -m pytest shared/ stocks/ funds/ -v

# Scrape stocks (dohod.ru + smartlab.ru)
cd scripts && python3 -m stocks.scrape

# Scrape fund distributions (Parus Google Sheets)
cd scripts && python3 -m funds.scrape
```

## Architecture

```
scripts/
  shared/          # network (retry), io (json save), dates (parsing)
  stocks/
    dohod.py       # parser for dohod.ru HTML
    smartlab.py    # parser for smartlab.ru HTML
    scrape.py      # entry point — runs both, saves separately
  funds/
    parus.py       # parser for Parus Google Sheets CSV
    scrape.py      # entry point — 8 Parus funds
data/
  stocks/dohod/    # raw dohod.ru data per ticker
  stocks/smartlab/ # raw smartlab.ru data per ticker
  funds/distributions/  # raw Parus fund data per fund
```

Each source saves to its own directory with its own index.json.

## CI

- **Stocks**: 1st & 15th of month, 09:00 UTC (`update-dividends.yml`)
- **Funds**: 5th of month, 09:00 UTC (`update-funds.yml`)

## Gotchas

- Smartlab blocks requests from GitHub Actions IPs — works locally only
- Google Sheets CSV needs `resp.content.decode('utf-8')`, not `resp.text` (wrong default encoding)
- Parus dates use Russian text months ("14 мар 2026"), not DD.MM.YYYY
- All parsers run from `scripts/` directory (imports are relative to it)
- After data structure changes, always propose a sync prompt for the heroincome app
