# Game Analytics Pipeline (Assignment 4)

ELT-пайплайн для ігрової аналітики. Збирає дані з трьох різних джерел (реляційна БД, сирі JSON-події, S3-сумісне object storage), складає у локальний аналітичний warehouse, і поверх нього будує layered dbt-проєкт з фактами, вимірами і window-функціями — все оркеструє Airflow з двома DAG-ами на різних розкладах.

Проєкт демонструє класичну medallion-архітектуру (raw → staging → mart) на компактному, "локальному" стеку: без хмарних сервісів, повністю в Docker Compose, але з тими ж патернами, які використовують продакшн-пайплайни.

## Що робить проєкт

Симулюється free-to-play мобільна гра:

- гравці реєструються, заходять у сесії, грають матчі, качають левели, переглядають рекламу, отримують нагороди — усе це летить у вигляді **event stream** (NDJSON, 32k+ подій, 11 типів);
- ті самі гравці роблять **покупки** через сторонній платіжний шлюз, і транзакції падають у реляційну **MySQL**;
- бізнес хоче щодня/щогодини бачити revenue, LTV гравців, топ-країни, 7-day rolling тренди.

Пайплайн бере ці сирі потоки, нормалізує їх, збагачує статичними довідниками (країни, курси валют, категорії продуктів, OS) і віддає аналітику готовий mart-шар з таблицями, які можна просто `SELECT *` — без джойнів і оконної магії на рантаймі.

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
              │   schema: main_stg   │   каст типів, фільтри по event_name
              └──────────┬───────────┘
                         │
                         ▼
              ┌──────────────────────┐
              │  mart-шар (tables)   │   7 моделей, tag:daily
              │   schema: main_mart  │   window functions, агрегації
              └──────────────────────┘
```

Пайплайн свідомо побудований на **cast-once-at-boundary** принципі: усі перетворення типів і нормалізація імен відбуваються на межі raw→stg; mart-шар уже працює з чистими, передбачуваними типами і не робить жодної роботи з "сирими" даними.

## Три джерела даних

Одна з цілей завдання — показати роботу з різними типами джерел. Кожне джерело має свій механізм вивантаження:

**1. MySQL → `raw.purchase_transactions`**

OLTP-база платіжного шлюзу. Python-скрипт `scripts/my_sql_to_duck.py` через `MySqlHook.get_pandas_df()` забирає таблицю `purchase_transactions` у pandas-датафрейм, а далі DuckDB зберігає його як таблицю через `con.register(...) + CREATE OR REPLACE TABLE`. Pandas тут виступає processing framework — саме цього вимагає умова завдання.

**2. NDJSON → `raw.events`**

Файл `data/raw/game_events.json` — сирий event stream у форматі newline-delimited JSON. DuckDB читає його нативно через `read_json_auto(..., format='newline_delimited')` за один виклик, без проміжних Python-обʼєктів. Це приклад schema-on-read джерела: поля різних типів подій (`player_registered`, `match_started`, `ad_watched`, ...) DuckDB склеює в єдину таблицю з `NULL` у колонках, яких у конкретній події нема.

**3. MinIO → `raw.country_codes`, `raw.fx_rates`, ...**

Чотири CSV-довідники лежать у bucket-і `game-analytics-seeds` у MinIO — локальному S3-сумісному object storage. DuckDB через екстеншн `httpfs` ходить туди напряму по `s3://` URL-ах, ніби це AWS S3. Це заміна "dbt seeds" — патерн, який масштабується на реальні great-great-large файли, тоді як seeds у dbt задумані для дрібних лук-апів розміру сотень рядків.

## Layered dbt-проєкт

Проєкт описаний у `dbt/dbt_project.yml`, профіль в `dbt/profiles.yml` вказує на DuckDB-файл. Усі моделі розкладені по трьох папках — raw / stg / mart — і кожен шар має свою матеріалізацію, схему і тег за замовчуванням.

### Raw layer

Це не моделі, а тільки декларації **sources** у `dbt/models/raw/sources.yml`. Фізично таблиці там створюють Airflow-таски, не dbt. Sources потрібні тільки для того, щоб dbt знав про raw-таблиці, міг запускати `source freshness`-тести і будувати lineage у документації.

Разом 6 sources у схемі `raw`:

| Source | Джерело | Опис |
|---|---|---|
| `purchase_transactions` | MySQL | сирі транзакції з платіжного шлюзу |
| `events` | NDJSON | сирий event stream (11 типів подій) |
| `country_codes` | MinIO | ISO2 → country name, region |
| `product_category_groups` | MinIO | product_category → бізнес-група |
| `platform_os_family` | MinIO | platform → OS family, mobile flag |
| `fx_rates` | MinIO | currency_code → rate_to_usd |

### Staging layer (13 моделей)

Матеріалізація: **views**, тег `hourly`. Схема: `main_stg`.

Дві базові моделі — `stg_events` і `stg_purchases` — роблять каст типів, нормалізують назви колонок і додають `event_date`/`transaction_date` на основі таймстемпів. Це ті точки, де raw типи перетворюються на аналітичні.

Ще 11 моделей — це тонкі **event-specific view** над `stg_events`: кожна фільтрує по одному `event_name` і залишає тільки колонки, релевантні для цього типу події. Наприклад, `stg_registrations` тягне `device_type` і `acquisition_channel`, а `stg_ads_watched` тягне `ad_network`, `ad_duration_seconds` і `reward_granted`. Так mart-шар не мусить тягнути цей фільтр кожен раз.

Повний список: `stg_events`, `stg_purchases`, `stg_registrations`, `stg_sessions_started`, `stg_sessions_ended`, `stg_matches_started`, `stg_matches_ended`, `stg_level_ups`, `stg_ad_offers_shown`, `stg_ads_watched`, `stg_chests_opened`, `stg_rewards_claimed`, `stg_shop_offers_viewed`.

### Mart layer (7 моделей)

Матеріалізація: **tables**, тег `daily`. Схема: `main_mart`. Саме цей шар бачить аналітик.

**`dim_dates`** — календарний вимір. Генерується динамічно через `generate_series` від найранішої події (з `stg_events` або `stg_purchases`) до `current_date`. Містить `date_day`, `day_of_week`, `is_weekend`, `month`, `year`. Використовується для join-у з фактами там, де треба показати "дні без подій".

**`dim_players`** — вимір гравців. Ключовий момент: гравці можуть існувати в events без покупок і навпаки, тому базою є `FULL OUTER JOIN` двох агрегованих CTE. Далі модель збагачує записи через join з `country_codes` (регіон) і `platform_os_family` (mobile/desktop). Виводить `is_payer`, `purchase_count`, `total_gross_usd`, `first_seen_ts`, `last_seen_ts`.

**`dim_products`** — SKU-каталог. Агрегує унікальні SKU з `stg_purchases`, джойниться з `product_category_groups` щоб отримати `category_group` і `is_subscription`. Містить лайф-тайм статистики: `times_sold`, `total_gross_usd`, `first_sold_ts`.

**`fct_daily_revenue`** — щоденний revenue. Завжди в USD через `fx_rates`. Колонки: `orders`, `unique_payers`, `gross_revenue_usd`, `avg_order_value_usd`. Єдина модель з **двома тегами** одночасно — успадкованим `daily` і явним `hourly` через `{{ config(tags=['hourly']) }}` у файлі. Це значить, що і hourly, і daily DAG-и її перебудовують: hourly — щоб аналітик бачив сьогоднішній revenue "на зараз", daily — щоб нічний перерахунок гарантовано переписав попередні дні на випадок late-arriving даних.

**`fct_player_ltv`** — life-time value гравців з **window function**:

```sql
sum(gross_usd) OVER (
    PARTITION BY player_id
    ORDER BY transaction_ts
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS cumulative_ltv_usd
```

Кожен рядок — одна транзакція, з поточним кумулятивним LTV цього гравця на момент покупки. Додатково `row_number()` дає `purchase_sequence_number`. Така форма (rows per purchase, а не один рядок на гравця) дозволяє будувати криві LTV у часі, а не тільки фінальне число.

**`fct_revenue_rolling_7d`** — 7-day rolling window поверх `fct_daily_revenue`:

```sql
sum(gross_revenue_usd) OVER (
    ORDER BY date_day
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
) AS rolling_7d_revenue_usd,

gross_revenue_usd - lag(gross_revenue_usd, 1) OVER (ORDER BY date_day)
    AS revenue_delta_vs_prev_day
```

Два класичні сценарії одним махом: згладжене середнє і day-over-day дельта через `lag()`.

**`fct_top_players_by_country`** — рейтинг топ-гравців усередині країни через `rank()`:

```sql
rank() OVER (PARTITION BY country_code ORDER BY total_gross_usd DESC)
    AS country_rank,

sum(total_gross_usd) OVER (PARTITION BY country_code)
    AS country_total_usd
```

Плюс похідне поле `share_of_country_revenue = total_gross_usd / country_total_usd`, яке відповідає на питання "скільки % revenue країни дає один whale".

Разом: **20 моделей** (13 stg + 7 mart) + 6 sources. Усі моделі мають тести в `schema.yml` — `not_null`/`unique` на PK, `accepted_values` на enum-поля (`region`, `platform`, `payment_status`), `relationships` на foreign key до довідників і dim-моделей. Це дає ~113 тестів, які `dbt build` прогонить автоматично після кожної збірки.

## MinIO як джерело довідкових CSV

Замість dbt seeds проєкт використовує **MinIO** — S3-сумісне локальне object storage. Це важливо архітектурно:

- dbt seeds задумані як маленькі допоміжні довідники, які живуть прямо в репо (сотні рядків). На великих файлах вони стають повільними і некерованими.
- Реальні пайплайни тримають такі CSV у хмарному object store (S3, GCS, Azure Blob) і читають їх з пайплайна.
- MinIO — це той самий S3 API, але локально; dbt/DuckDB не бачать різниці і ходять туди саме через S3-протокол.

Як воно з'єднано:

1. `docker compose up` піднімає сервіс `minio` та одноразовий допоміжний контейнер `minio-init` на базі `minio/mc` (офіційного MinIO CLI).
2. `minio-init` створює bucket `game-analytics-seeds` і заливає туди 4 CSV-файли з локальної папки `./data/seeds/`. Контейнер завершується, bucket залишається в томі `minio-data`.
3. Airflow-таска `load_minio_to_duckdb` (перша в hourly DAG) через `httpfs` екстеншн DuckDB читає ці CSV напряму з `s3://game-analytics-seeds/...` і робить `CREATE OR REPLACE TABLE raw.<name>`.
4. Далі dbt-моделі читають їх як звичайні `{{ source('raw', 'country_codes') }}`, нічого не знаючи ні про MinIO, ні про S3.

### Як зайти в MinIO і подивитись, що там

MinIO має повноцінну **веб-консоль**, яка виглядає як AWS S3 Console.

1. Відкрити в браузері: **http://localhost:9001**
2. Логін / пароль: `minioadmin` / `minioadmin`
3. У лівому сайдбарі — **Object Browser**. З'явиться список бакетів.
4. Клік на bucket **`game-analytics-seeds`** — всередині 4 файли:
   - `country_codes.csv`
   - `fx_rates.csv`
   - `platform_os_family.csv`
   - `product_category_groups.csv`
5. По кліку на файл доступні: **Preview** (перегляд у браузері), **Download**, **Delete**, **Share**, історія версій, метадані (розмір, дата, etag).
6. Новий файл можна залити через drag-and-drop у вікно bucket-а.

MinIO слухає два порти:

- **9000** — S3 API. Саме на нього ходить DuckDB як `http://minio:9000` (внутрішнє ім'я всередині docker-мережі).
- **9001** — веб-консоль для людей (`http://localhost:9001` на хості).

Креди для airflow/DuckDB передаються через env у `docker-compose.yml`: `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_BUCKET`. Скрипт `scripts/load_minio_to_duck.py` читає їх з environment, а не зашиває в код, тому продовження в реальний AWS S3 = просто поміняти ендпоінт і креди.

Демо-сценарій "дані оновились в object store": відкриваєш MinIO-консоль → замінюєш, наприклад, `fx_rates.csv` через drag-and-drop → тригериш `game_analytics_hourly` → `load_minio_to_duckdb` перезапише `raw.fx_rates`, а всі mart-моделі, що джойняться з нею, автоматично візьмуть нові курси на наступному білді.

## Airflow: два DAG-и з різним розкладом

Завдання вимагає показати роботу з тегами dbt і з різними розкладами. Рішення — два окремі DAG-и, які працюють з одним warehouse.

**`game_analytics_hourly`** — `schedule="@hourly"`

```
load_minio_to_duckdb → extract_mysql_to_duckdb → load_json_to_duckdb → dbt_build_hourly
```

Це "операційний" DAG. Кожну годину перетягує свіжі довідники з MinIO, транзакції з MySQL, події з NDJSON, потім робить `dbt build --select tag:hourly`. Під тег `hourly` попадає весь stg-шар (через `+tags: ['hourly']` у `dbt_project.yml`) плюс `fct_daily_revenue`, який має цей тег явним override-ом. Ідея — щоб аналітик завжди бачив сьогоднішній revenue оновленим.

**`game_analytics_daily`** — `schedule="15 3 * * *"`

```
dbt_build_daily → generate_analytics_summary
```

Нічний DAG для важких mart-агрегацій. `dbt build --select tag:daily` перебудовує `dim_dates`, `dim_players`, `dim_products`, `fct_player_ltv`, `fct_revenue_rolling_7d`, `fct_top_players_by_country` (і знову `fct_daily_revenue`, бо він в обох тегах — спеціально, щоб нічний ран переписав попередні дні з урахуванням late-arriving даних). Потім Python-таска `generate_analytics_summary` збирає підсумки для звіту.

**Чому саме два DAG-и, а не один.** DuckDB тримає файл ексклюзивно під запис, тому два паралельних таски в нього не можуть писати. Cron-офсет `:15` у daily DAG-а гарантує, що він не впирається в DuckDB-лок з hourly DAG-ом, який стартує на рівній годині. Обидва DAG-и мають `max_active_runs=1`, щоб послідовні рани не перекривались самі з собою.

`dbt build` (а не `dbt run + dbt test` окремо) — це свідомий вибір: `build` під капотом робить `run + test + snapshot` у правильному порядку з урахуванням залежностей, і дає єдиний exit-code на всю збірку.

## Технологічний стек

| Компонент | Версія | Роль |
|---|---|---|
| Airflow | 2.10.5 (LocalExecutor) | оркестрація |
| Postgres | 16 | метадата Airflow |
| MySQL | 8.0 | джерело транзакцій |
| MinIO | latest | S3-сумісне object storage |
| DuckDB | 1.0 | аналітичний warehouse |
| dbt-duckdb | 1.8.3 | трансформації |
| pandas | 2.x | processing framework для MySQL→DuckDB |
| polars | 1.6 | альтернативний DataFrame (не використаний у фінальній версії) |

Кастомний образ Airflow (`Dockerfile`) додає до базового `apache/airflow:2.10.5`: `git`, `libmysqlclient-dev`, `apache-airflow-providers-mysql`, `dbt-core`, `dbt-duckdb`, `duckdb`, `polars`.

## Структура репозиторію

```
.
├── Dockerfile                          # кастомний airflow-образ
├── docker-compose.yml                  # 8 сервісів (postgres, mysql, minio, minio-init,
│                                       #             airflow-init/webserver/scheduler, dbt-docs)
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
│   ├── raw/game_events.json            # NDJSON з ~32k подій
│   └── seeds/                          # CSV-довідники (монтуються в minio-init)
│
├── include/sql/mysql/                  # init-SQL для MySQL (DDL + sample data)
├── duckdb/                             # warehouse.duckdb (локально, не комітиться)
└── logs/                               # airflow-логи (не комітяться)
```

## dbt docs

Сервіс `dbt-docs` стартує разом зі стеком і віддає згенеровану документацію на **http://localhost:8081**. Там видно lineage-граф (raw → stg → mart → tests), описи моделей з `schema.yml`, компільований SQL кожної моделі, результати тестів і метадані колонок. Це основний артефакт для демо: одним кліком видно, як дані течуть від MinIO/MySQL/JSON до `fct_top_players_by_country`.

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
SELECT country_code, player_id, total_gross_usd, share_of_country_revenue
FROM main_mart.fct_top_players_by_country
WHERE country_rank = 1
ORDER BY total_gross_usd DESC;

-- Динаміка: revenue + 7-day rolling + day-over-day дельта
SELECT date_day, gross_revenue_usd, rolling_7d_revenue_usd, revenue_delta_vs_prev_day
FROM main_mart.fct_revenue_rolling_7d
ORDER BY date_day DESC LIMIT 14;

-- Розподіл гравців за регіоном: скільки payer-ів, середній LTV
SELECT region, count(*) AS players,
       sum(CASE WHEN is_payer THEN 1 ELSE 0 END) AS payers,
       avg(total_gross_usd) AS avg_revenue_per_player
FROM main_mart.dim_players
GROUP BY region
ORDER BY avg_revenue_per_player DESC;
```

## Короткий запуск

Для тих, хто хоче просто підняти і подивитись:

```bash
echo "AIRFLOW_UID=50000" > .env
docker compose build airflow-webserver
docker compose up airflow-init
docker compose up -d
```

Далі **http://localhost:8080** (Airflow, `admin`/`admin`), **http://localhost:9001** (MinIO, `minioadmin`/`minioadmin`), **http://localhost:8081** (dbt docs). У Airflow unpause обидва DAG-и, тригернути спочатку `game_analytics_hourly`, потім `game_analytics_daily` (mart-моделі залежать від stg, тому daily на cold start без hourly впаде).
