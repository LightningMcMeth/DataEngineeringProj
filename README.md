# Game Analytics Pipeline (Assignment 4)

ELT-пайплайн для ігрової аналітики на **Airflow + dbt + DuckDB + MinIO**.
Три різні джерела (MySQL, NDJSON-файл, CSV-довідники у MinIO) зливаються в raw-шар DuckDB, далі dbt будує stg- і mart-шари з window-функціями, а Airflow тримає два DAG-и з різним розкладом.

## Архітектура

```
┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│   MySQL      │   │   NDJSON     │   │   MinIO      │
│ (purchases)  │   │  (events)    │   │ (довідники)  │
└──────┬───────┘   └──────┬───────┘   └──────┬───────┘
       │                  │                  │
       │ pandas           │ read_json_auto   │ httpfs / s3
       ▼                  ▼                  ▼
    ┌────────────────────────────────────────────┐
    │         DuckDB — схема `raw`               │
    │  raw.purchase_transactions                 │
    │  raw.events                                │
    │  raw.country_codes                         │
    │  raw.product_category_groups               │
    │  raw.platform_os_family                    │
    │  raw.fx_rates                              │
    └────────────────────┬───────────────────────┘
                         │ dbt (sources)
                         ▼
              ┌──────────────────────┐
              │   stg-шар (views)    │   13 моделей, tag:hourly
              │   schema: main_stg   │
              └──────────┬───────────┘
                         │
                         ▼
              ┌──────────────────────┐
              │  mart-шар (tables)   │   7 моделей, tag:daily
              │   schema: main_mart  │   (window functions)
              └──────────────────────┘
```

## Стек

- **Airflow 2.10.5** (LocalExecutor, Postgres як метадата)
- **MySQL 8.0** — джерело транзакцій (`purchase_transactions`)
- **MinIO (latest)** — S3-сумісне сховище для довідкових CSV (заміна dbt seeds)
- **DuckDB 1.0** — аналітичне сховище (`/opt/airflow/duckdb/warehouse.duckdb`)
- **dbt-duckdb 1.8** — трансформації, тести, docs
- Кастомний Airflow-образ описаний у `Dockerfile` (git, libmysqlclient, mysql-provider, dbt, duckdb, polars)

## Структура проєкту

```
.
├── Dockerfile                          # кастомний airflow-образ
├── docker-compose.yml                  # 8 сервісів: postgres, mysql, minio, minio-init,
│                                       #             airflow-init/webserver/scheduler, dbt-docs
├── .env                                # локальні креди (не комітиться)
│
├── dags/
│   ├── game_analytics_pipeline_dag.py  # DAG id: game_analytics_hourly
│   └── game_analytics_daily_dag.py     # DAG id: game_analytics_daily
│
├── scripts/
│   ├── my_sql_to_duck.py               # MySQL → raw.purchase_transactions
│   ├── load_json_to_duck.py            # NDJSON → raw.events
│   ├── load_minio_to_duck.py           # MinIO (S3) → raw.country_codes / fx_rates / ...
│   └── generate_analytics_summary.py   # python-посткрок після daily dbt build
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── raw/sources.yml             # 6 sources (2 з БД + 4 з MinIO)
│   │   ├── stg/                        # 13 моделей, views, tag:hourly
│   │   └── mart/                       # 7 моделей, tables, tag:daily
│   ├── macros/
│   └── tests/
│
├── data/
│   ├── raw/game_events.json            # NDJSON з подіями
│   └── seeds/                          # CSV-довідники (монтуються в minio-init)
│       ├── country_codes.csv
│       ├── product_category_groups.csv
│       ├── platform_os_family.csv
│       └── fx_rates.csv
│
├── include/sql/mysql/                  # init-SQL для MySQL
├── duckdb/                             # warehouse.duckdb (локально, не комітиться)
└── logs/                               # airflow-логи (не комітяться)
```

## Як підняти з нуля

1. Склонувати репо і зайти в папку:
   ```bash
   git clone <repo_url>
   cd "Assignment 4"
   ```
2. Створити файл `.env` (див. розділ нижче).
3. Збілдити образ і підняти стек:
   ```bash
   docker compose build airflow-webserver
   docker compose up airflow-init     # одноразово: створює БД Airflow і юзера admin/admin
   docker compose up -d
   ```
4. Перевірити, що все піднялось:
   ```bash
   docker compose ps
   ```
   Повинні бути `Up`: postgres, mysql, minio, airflow-webserver, airflow-scheduler, dbt-docs.
   Контейнер `minio-init` одноразовий — запустився, залив CSV у bucket і вийшов (це норма).

5. Відкрити Airflow: **http://localhost:8080**, логін `admin` / `admin`.
6. Conn `mysql_game_db` підставляється автоматично через `docker-compose.yml` (env `AIRFLOW_CONN_MYSQL_GAME_DB`), вручну не треба.
7. Знайти в UI DAG-и `game_analytics_hourly` і `game_analytics_daily`, unpause, потім `Trigger DAG` на hourly → чекаєш зелене → потім на daily.

## `.env`

Файл не комітиться. Мінімальний вміст:

```
AIRFLOW_UID=50000
```

## DAG-и

**`game_analytics_hourly`** — schedule `@hourly`:

```
load_minio_to_duckdb → extract_mysql_to_duckdb → load_json_to_duckdb → dbt_build_hourly
```

Вантажить довідники з MinIO, транзакції з MySQL, події з NDJSON у `raw.*`, потім `dbt build --select tag:hourly` перебудовує весь stg-шар і операційні mart-моделі (наприклад, `fct_daily_revenue`).

**`game_analytics_daily`** — schedule `15 3 * * *`:

```
dbt_build_daily → generate_analytics_summary
```

`dbt build --select tag:daily` перебудовує важкі mart-агрегації (`dim_players`, `fct_player_ltv`, `fct_revenue_rolling_7d`, `fct_top_players_by_country`), потім Python-таска збирає summary. Cron з офсетом `:15` замість `:00`, щоб не впиратись у DuckDB-lock з hourly DAG-ом, який стартує на рівній годині.

## dbt-моделі

**Raw sources (6)** — декларовані в `dbt/models/raw/sources.yml`, всі живуть у схемі `raw`:

- `purchase_transactions` — з MySQL
- `events` — з NDJSON
- `country_codes`, `product_category_groups`, `platform_os_family`, `fx_rates` — з MinIO

**Staging (13, views, tag:hourly)** — `dbt/models/stg/`:

- `stg_events` — базова view над `source('raw', 'events')`, каст типів, `event_date`.
- `stg_purchases` — базова view над `source('raw', 'purchase_transactions')`.
- 11 event-specific view-фільтрів: `stg_registrations`, `stg_sessions_started/ended`, `stg_matches_started/ended`, `stg_level_ups`, `stg_ad_offers_shown`, `stg_ads_watched`, `stg_chests_opened`, `stg_rewards_claimed`, `stg_shop_offers_viewed`.

**Mart (7, tables, tag:daily)** — `dbt/models/mart/`:

- `dim_dates` — календарний вимір, генерується від найранішої події до сьогодні.
- `dim_players` — `FULL OUTER JOIN` гравців з events + purchases, збагачений через `country_codes` і `platform_os_family` з MinIO.
- `dim_products` — вимір SKU, збагачений через `product_category_groups` з MinIO.
- `fct_daily_revenue` — щоденний revenue, `gross_usd` через `fx_rates`. Має обидва теги (`daily` + `hourly` через override в моделі), щоб оновлюватись і операційно, і вночі.
- `fct_player_ltv` — кумулятивний LTV через `sum() OVER (PARTITION BY player_id ORDER BY transaction_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`.
- `fct_revenue_rolling_7d` — 7-day rolling revenue + `lag()` для day-over-day дельти.
- `fct_top_players_by_country` — `rank() OVER (PARTITION BY country_code ORDER BY total_gross_usd DESC)` + частка revenue гравця від revenue країни.

Разом: **20 моделей** (13 stg + 7 mart) + 6 sources.

## MinIO як джерело CSV (бонус +2.5)

Довідкові CSV (`country_codes`, `fx_rates`, `platform_os_family`, `product_category_groups`) не зберігаються як `dbt seeds`, а лежать у MinIO — S3-сумісному object storage, який імітує реальні сценарії роботи з великими даними.

Як воно працює:

1. `docker compose up` піднімає сервіс `minio` + одноразовий допоміжний контейнер `minio-init` на базі `minio/mc`.
2. `minio-init` створює bucket `game-analytics-seeds` і заливає туди 4 CSV-файли з локальної папки `./data/seeds/`.
3. Airflow-таска `load_minio_to_duckdb` (перша в hourly DAG) через `httpfs` екстеншн DuckDB читає ці CSV напряму з `s3://game-analytics-seeds/...` і робить `CREATE OR REPLACE TABLE raw.<name>`.
4. Далі dbt-моделі читають їх як звичайні `{{ source('raw', 'country_codes') }}`, ніби це просто таблиці в базі.

## dbt docs

Сервіс `dbt-docs` стартує разом зі стеком і віддає згенеровану документацію на **http://localhost:8081**. Там видно lineage-граф (raw → stg → mart), описи моделей, SQL кожної моделі, результати тестів.

## Корисні команди

```bash
# перевірити, що raw-шар наповнився
docker compose exec airflow-scheduler bash -lc "python -c \"
import duckdb
c = duckdb.connect('/opt/airflow/duckdb/warehouse.duckdb', read_only=True)
for t in ['events','purchase_transactions','country_codes','fx_rates',
          'platform_os_family','product_category_groups']:
    print(t, c.execute(f'SELECT COUNT(*) FROM raw.{t}').fetchone())
\""

# sanity check dbt
docker compose exec airflow-scheduler bash -lc "cd /opt/airflow/dbt && dbt debug"

# запустити dbt вручну
docker compose exec airflow-scheduler bash -lc "cd /opt/airflow/dbt && dbt build --target dev"

# інтерактивний DuckDB CLI
docker compose exec airflow-scheduler bash -lc "duckdb /opt/airflow/duckdb/warehouse.duckdb"

# рестарт тільки airflow
docker compose restart airflow-webserver airflow-scheduler

# логи сервісу
docker compose logs -f airflow-scheduler
docker compose logs minio-init          # перевірити, що bucket створився і файли залились

# зупинити все, зберегти дані
docker compose down

# повний ресет (стирає mysql/postgres/minio томи!)
docker compose down -v
```

## Приклади аналітичних запитів

```sql
-- Топ-10 днів за revenue
SELECT * FROM main_mart.fct_daily_revenue
ORDER BY gross_revenue_usd DESC LIMIT 10;

-- Топ-5 whales по LTV
SELECT player_id, max(cumulative_ltv_usd) AS ltv_usd
FROM main_mart.fct_player_ltv
GROUP BY player_id
ORDER BY ltv_usd DESC LIMIT 5;

-- Whale #1 у кожній країні
SELECT country_code, player_id, total_gross_usd
FROM main_mart.fct_top_players_by_country
WHERE country_rank = 1
ORDER BY total_gross_usd DESC;

-- Динаміка: revenue + 7-day rolling + day-over-day дельта
SELECT date_day, gross_revenue_usd, rolling_7d_revenue_usd, revenue_delta_vs_prev_day
FROM main_mart.fct_revenue_rolling_7d
ORDER BY date_day DESC LIMIT 14;

-- Частка payer-ів серед усіх гравців
SELECT is_payer, count(*) AS players
FROM main_mart.dim_players
GROUP BY is_payer;
```

