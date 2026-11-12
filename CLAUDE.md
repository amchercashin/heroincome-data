# heroincome-data

Pure data repository — stores raw dividend/distribution data from external sources.
No business logic, no merge, no prioritization. Consuming apps decide how to use data.

## Commands

```bash
# Tests (run from scripts/)
cd scripts && python3 -m pytest shared/ stocks/ funds/ -v

# Scrape stocks (dohod.ru)
cd scripts && python3 -m stocks.scrape

# Scrape stocks (smart-lab.ru, ~6 minutes: ~220 pages, 1.5 s apart)
cd scripts && python3 -m stocks.scrape_smartlab

# Scrape fund distributions (parus.properties, Google Sheets fallback)
cd scripts && python3 -m funds.scrape
```

## Architecture

```
scripts/
  shared/          # network (retry), io (json save), dates (parsing)
  stocks/
    dohod.py       # parser for dohod.ru HTML
    scrape.py      # entry point (dohod.ru)
    smartlab.py    # smart-lab.ru: shares list → issuer pages, dividend tables, calendar statuses
    scrape_smartlab.py  # entry point (smart-lab.ru)
    fixtures/      # trimmed real smart-lab pages for the tests
  funds/
    parus_site.py  # parus.properties: fund list + payouts table per fund page
    parus.py       # Parus Google Sheets CSV (fallback for the 8 funds known before)
    scrape.py      # entry point — site first, sheets for what the site missed, MOEX secid → aliases
data/
  stocks/dohod/    # raw dohod.ru data per ticker
  stocks/smartlab/ # raw smart-lab.ru data per ticker (not read by the app yet)
  funds/distributions/  # raw Parus fund data per fund, file name = ISIN
```

Each source saves to its own directory with its own index.json.
smart-lab.ru came back in Nov 2026 as a second stock source: it had been removed in Oct 2026
because it blocked GitHub Actions IPs, but a 70-page probe from Actions on 2026-11-12 passed
without a block. investmint.ru is not scraped: its user agreement (5.1.5, 6.3) forbids automated
collection and redistribution without written permission.

## CI

- **Stocks**: every Monday 05:17 UTC (`update-dividends.yml`)
- **Funds**: every Monday 05:47 UTC (`update-funds.yml`)
- **smart-lab**: every Monday 06:17 UTC (`update-smartlab.yml`); the run fails (and commits nothing) when the shares list or the calendar is unavailable, 8 pages in a row fail, or no page parses — a failed scheduled run emails the repo owner
- Both commit through `.github/scripts/commit-data.sh <dir> <message>`: it commits only when something besides `scrapedAt` / `updatedAt` changed (every run rewrites those), and does `git pull --rebase` before pushing because the two workflows may push close together. Keep any new scraper's workflow on the same script.
- Consumer: the Рантье app (github.com/amchercashin/rantie) reads `data/` from raw.githubusercontent.com on every sync; since autumn 2026 MOEX ISS gives no free dividends, so this repo is its only source of stock dividends and Parus distributions.

## Gotchas

- Google Sheets CSV needs `resp.content.decode('utf-8')`, not `resp.text` (wrong default encoding)
- Parus dates use Russian text months ("14 мар 2026"), not DD.MM.YYYY; sheets have typos ("фер") — add aliases to `MONTHS_RU`; planned payment dates have no year ("~10.12 (план)") — it is taken from the record date
- Funds: files and index keys are ISINs; a MOEX secid that differs (XTRIUMF) goes to `aliases` in index.json. The index lists every file on disk, so a fund that fails one run keeps its data
- Planned payouts are published only after their register closes — the app reads every row as a real payment
- ПАРУС-МАКС has no payouts table (reinvests) and is skipped; ПАРУС-Сити / ПАРУС-ДМД trade on MOEX but have no page on parus.properties yet
- smart-lab sends no charset header: decode `resp.content` as UTF-8 (`resp.text` gives mojibake)
- smart-lab: a preferred share has no page of its own — its rows are on the common share's page (`/q/SBER/dividend/` holds SBER and SBERP); the page code comes from the `/q/shares/` table (`/q/{CODE}/f/y/`, same as `/forum/{CODE}`), which is not always the common ticker (WTCM → WTCMP). Rows are split by the ticker column
- smart-lab: the share page does not tell a board recommendation from a forecast — only the `/dividends/` calendar does (`dividend_approved` = green = recommended). Upcoming rows take their status from it; without the calendar the run stops instead of writing `expected` everywhere
- smart-lab: a share that never paid has only the yearly summary table (no «Выплаченные» table) — that is no rows, not a parse error; a page with no `table.dividends` at all is an error. Old rows of a renamed company are listed under the old ticker (ELFV page → ENRU); with one current ticker on the page they go to it with `listedAs`
- All parsers run from `scripts/` directory (imports are relative to it)
- After data structure changes, always propose a sync prompt for the heroincome app
