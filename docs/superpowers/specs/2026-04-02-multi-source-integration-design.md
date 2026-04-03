# Multi-Source Data Integration Design

Расширение heroincome-data с одного источника (dohod.ru, только акции) до мульти-источниковой архитектуры: акции, облигации, фонды, с возможностью добавления крипты.

## Архитектурный подход

**Подход C: разделение по типу инструмента.** Отдельный пайплайн на каждый тип инструмента со своими источниками, схемой и расписанием, но с общими утилитами (сеть, даты, I/O).

Обоснование: акции, облигации и фонды — принципиально разные сущности с разными источниками, частотой обновления и схемами данных. Единый `Payment`-тип был бы искусственным.

## 1. Структура проекта

```
heroincome-data/
├── scripts/
│   ├── shared/
│   │   ├── network.py       # fetch_with_retry, session setup
│   │   ├── io.py            # save_json, update_index
│   │   └── dates.py         # parse_date_dmy, parse_date_iso
│   ├── stocks/
│   │   ├── dohod.py         # парсер dohod.ru
│   │   ├── smartlab.py      # парсер smartlab.ru
│   │   ├── merge.py         # мерж данных из двух источников
│   │   ├── scrape.py        # точка входа
│   │   └── test_stocks.py
│   ├── bonds/
│   │   ├── moex_iss.py      # парсер MOEX ISS API
│   │   ├── scrape.py        # точка входа
│   │   └── test_bonds.py
│   ├── funds/
│   │   ├── parus.py         # парсер Google Sheets Парус
│   │   ├── scrape.py        # точка входа
│   │   └── test_funds.py
│   └── scrape.py            # legacy — удаляется после миграции
├── data/
│   ├── stocks/
│   │   ├── dividends/       # {TICKER}.json
│   │   └── index.json
│   ├── bonds/
│   │   ├── coupons/         # {SECID}.json
│   │   └── index.json
│   └── funds/
│       ├── distributions/   # {TICKER или ISIN}.json
│       └── index.json
```

## 2. Stocks: дивиденды по акциям

### Источники

- **dohod.ru** (primary) — текущий источник, 127 тикеров
- **smartlab.ru** (secondary) — 222 тикера, резервный источник, альтернативные прогнозы

### Smartlab: что берём

- `/q/{TICKER}/dividend/` — полная история выплат + ожидаемые
- `/dividends/` — список всех тикеров с дивидендами
- CSS-класс `dividend_approved` — разделение утверждённых СД vs прогнозных

### Схема данных

```json
{
  "ticker": "LKOH",
  "isin": "RU0009024277",
  "scrapedAt": "2026-04-02T10:00:00Z",
  "payments": [
    {
      "recordDate": "2026-01-12",
      "declaredDate": "2025-11-21",
      "amount": 397.0,
      "year": 2025,
      "status": "paid",
      "sources": {
        "dohod": { "amount": 397.0, "scrapedAt": "..." },
        "smartlab": { "amount": 397.0, "scrapedAt": "..." }
      }
    }
  ]
}
```

Изменения относительно текущей схемы:
- `isForecast` → `status`: `"paid"` | `"approved"` | `"forecast"`
- `isin` — опциональное поле (из MOEX ISS, если доступен). Приоритетный идентификатор для матчинга, когда есть. Если нет — матчим по `ticker` + `recordDate`.
- `sources` — сырые данные из каждого источника
- Верхнеуровневое `source` удаляется

### Мерж-логика

1. Матчинг выплат: по `isin` + `recordDate` (приоритет), fallback на `ticker` + `recordDate`
2. Приоритет `amount`: dohod.ru → smartlab. При расхождении — берём dohod, логируем
3. Приоритет `status`: `paid` > `approved` > `forecast`
4. Тикеры только из одного источника — берутся как есть
5. Fallback: если один источник недоступен — данные из другого

## 3. Bonds: купоны по облигациям

### Источник

MOEX ISS API — бесплатный, структурированный JSON, без авторизации.

- Listing: `/iss/engines/stock/markets/bonds/securities.json`
- Per-security: `/iss/securities/{SECID}/bondization.json?limit=200`
- Metadata: `/iss/securities/{SECID}.json`

### Схема данных

```json
{
  "secid": "SU26238RMFS4",
  "isin": "RU000A1038V6",
  "name": "ОФЗ 26238",
  "faceValue": 1000.0,
  "currency": "RUB",
  "matDate": "2041-05-28",
  "bondType": "ofz_bond",
  "scrapedAt": "2026-04-02T10:00:00Z",
  "coupons": [
    {
      "couponDate": "2026-06-04",
      "recordDate": "2026-06-02",
      "value": 33.91,
      "valuePrc": 6.9,
      "startDate": "2025-12-04"
    }
  ],
  "amortizations": [
    {
      "amortDate": "2041-05-28",
      "value": 1000.0,
      "valuePrc": 100.0,
      "type": "maturity"
    }
  ],
  "offers": [
    {
      "offerDate": "2028-06-01",
      "offerType": "put",
      "value": 1000.0
    }
  ]
}
```

Ключевые решения:
- `secid` — основной идентификатор (MOEX-нативный), `isin` — опциональный
- Файлы: `data/bonds/coupons/{SECID}.json`
- Флоатеры: будущие купоны с `value: null` — сохраняем как есть
- Амортизации и оферты включены в тот же файл
- Дополнительные источники (rusbonds.ru, cbonds.ru) — не добавляем пока

### Стратегия обновления

- Полная загрузка при первом запуске: перебор всех SECID из listing
- Обновление: только активные (не погашенные) облигации
- Частота: раз в неделю

## 4. Funds: распределения фондов

### Источники

1. **Парус** (8 фондов) — Google Sheets CSV export. Один HTTP GET на фонд, полная история. Без авторизации.
2. **Остальные УК** — добавляются позже, каждая как отдельный мини-парсер
3. **MOEX ISS** — только для метаданных фондов (ISIN, название, тип), НЕ для распределений

### Scope первой версии

Только Парус (8 фондов через Google Sheets). Остальные УК — позже.

### Google Sheet IDs (Парус)

| Фонд | ISIN | Sheet ID |
|------|------|----------|
| ПАРУС-ОЗН | RU000A1022Z1 | `1EBBlo_L-h1X1zkvybI-cPh-gPjS4mUbJ4NkrkLTeRd4` |
| ПАРУС-СБЛ | RU000A104172 | `1dcOHCw6t2C2BnQep4x6-Dc71VivN03JpbgQ6KBP3Uvw` |
| ПАРУС-НОРДВЕЙ | RU000A104KU3 | `14ImcLbbh8wSVwiohuYbpJIS04jfV3x-GdXTrIXeo1FU` |
| ПАРУС-ЛОГИСТИКА | RU000A105328 | `1_Jqoal_hmJ0jpDHutasR5QarJfPD8RhXYb3QdcmMBak` |
| ПАРУС-ДВИНЦЕВ | RU000A1068X9 | `1G1Eusuay0PU4aYYohxI3jMl1bJYok8bsTBajNeqx5bI` |
| ПАРУС-КРАСНОЯРСК | RU000A108UH0 | `1RRQwzPScXeaQ7TXiOmeIjgnxoy4y9mkIgqHfJIhw18Y` |
| ПАРУС-ЗОЛЯ | RU000A10CFM8 | `1hQGBzKvDNHB0tnO0DrC0MNsgrYuuULjeGIVHfXyL_w4` |
| ПАРУС-ТРИУМФ | XTRIUMF | `1Egct-_5Bbi_LsHyidqL55i8EkT_nFvYgQ__hC6XyEDw` |

ПАРУС-МАКС (RU000A108VR7) — фонд реинвестирования, без распределений, не включаем.

### Схема данных

```json
{
  "isin": "RU000A1022Z1",
  "ticker": "PLZ5",
  "name": "ПАРУС-ОЗН",
  "managementCompany": "Parus",
  "scrapedAt": "2026-04-02T10:00:00Z",
  "distributions": [
    {
      "paymentDate": "2026-03-15",
      "recordDate": "2026-03-10",
      "unitPrice": 1250.0,
      "amountBeforeTax": 9.52,
      "amountAfterTax": 8.28,
      "yieldPrc": 0.76,
      "status": "paid"
    }
  ]
}
```

Ключевые решения:
- `isin` / `ticker` — оба опциональные
- Файлы: `{TICKER}.json` если есть тикер, иначе `{ISIN}.json`
- Суммы до и после НДФЛ — обе сохраняются
- Запланированные выплаты: `status: "planned"` | `"paid"`
- Частота обновления: раз в месяц

## 5. Shared-утилиты

Вынос из текущего `scripts/scrape.py`:

- `shared/network.py` — `fetch_with_retry` (retry с экспоненциальным backoff, User-Agent, таймауты)
- `shared/io.py` — `save_json(path, data)`, `update_index(path, items)`
- `shared/dates.py` — `parse_date_dmy(text)` (DD.MM.YYYY → YYYY-MM-DD), `parse_date_iso(text)`

## 6. CI/CD

Три независимых workflow:

```yaml
# .github/workflows/update-stocks.yml
# Расписание: 1 и 15 числа, 09:00 UTC
# Запускает: python scripts/stocks/scrape.py

# .github/workflows/update-bonds.yml
# Расписание: каждый понедельник, 09:00 UTC
# Запускает: python scripts/bonds/scrape.py

# .github/workflows/update-funds.yml
# Расписание: 5 числа каждого месяца, 09:00 UTC
# Запускает: python scripts/funds/scrape.py
```

Каждый workflow: независим, коммитит только свою папку в `data/`, поддерживает `workflow_dispatch`.

## 7. Порядок реализации

| Фаза | Что | Почему в таком порядке |
|------|-----|----------------------|
| 1 | Рефакторинг: вынос shared, перенос stocks, перенос данных | Фундамент для всего остального |
| 2 | Bonds (MOEX ISS) | Структурированный API, низкий риск |
| 3 | Funds (Парус Google Sheets) | Стабильный источник, 8 фондов |
| 4 | Smartlab (второй источник для акций) | HTML-скрейпинг + мерж — самая сложная часть |
| 5 | Crypto (будущее) | Отдельный пайплайн по той же схеме |

Каждая фаза — отдельный PR.

## 8. Синхронизация с приложением

После завершения каждой фазы необходимо синхронизировать приложение heroincome с изменениями в структуре данных. Это процессный шаг, не автоматизация.

**Обязательное действие после каждой фазы:** предложить пользователю готовый промт для запуска в контексте приложения heroincome, содержащий:
- Какие конкретно изменения произошли в структуре данных
- Какие новые поля / папки / схемы появились
- Что нужно обновить в приложении для корректного потребления данных
