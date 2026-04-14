# Game Analytics Pipeline (Assignment 4)

ELT-пайплайн на Airflow + dbt + DuckDB. MySQL і NDJSON з подіями → raw-шар у DuckDB → (далі) stg і mart через dbt.

## Стек

- **Airflow 2.10.5** (LocalExecutor, Postgres як метадата)
- **MySQL 8.0** — джерело транзакцій (`purchase_transactions`)
- **DuckDB 1.0** — аналітичне сховище (`/opt/airflow/duckdb/warehouse.duckdb`)
- **dbt-duckdb 1.8** — трансформації
- Кастомний Airflow-образ описаний у `Dockerfile` (git, libmysqlclient, mysql-provider, dbt, duckdb, polars)

## Структура

```
.
├── Dockerfile                 # кастомний airflow-образ
├── docker-compose.yml
├── .env                       # локальні креди (не комітити)
├── dags/
│   └── game_analytics_pipeline_dag.py
├── scripts/
│   ├── my_sql_to_duck.py      # MySQL -> raw.purchase_transactions
│   └── load_json_to_duck.py   # NDJSON -> raw.events
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/                # raw / stg / mart (в роботі)
│   ├── seeds/
│   ├── macros/
│   └── tests/
├── data/raw/game_events.json
├── include/sql/mysql/         # init-SQL для MySQL
├── duckdb/                    # warehouse.duckdb (локально, не комітиться)
└── logs/                      # airflow-логи (не комітяться)
```

## Як підняти з нуля

1. Склонувати репо і зайти в папку:
   ```bash
   git clone <repo_url>
   cd "Assignment 4"
   ```
2. Створити файл `.env` (див. розділ нижче).
3. Збілдити і підняти:
   ```bash
   docker compose build
   docker compose up airflow-init     # одноразово, створює БД Airflow і користувача admin/admin
   docker compose up -d
   ```
4. Відкрити `http://localhost:8080`, логін `admin` / `admin`.
5. У Airflow `Admin → Connections → +` створити конект до MySQL:
   - Conn Id: `mysql_game_db`
   - Conn Type: `MySQL`
   - Host: `mysql`
   - Schema: `game_db`
   - Login: `game_user`
   - Password: `game_pass`
   - Port: `3306`
   - (Якщо `Test` падає на `mysqlclient` — у полі Extra: `{"client": "mysql-connector-python"}`)
6. В UI знайти DAG `game_analytics_pipeline`, unpause, Trigger DAG. Обидва таски мають стати зеленими.
7. Перевірка raw-шару:
   ```bash
   docker compose exec airflow-scheduler bash -lc "python -c \"import duckdb; c=duckdb.connect('/opt/airflow/duckdb/warehouse.duckdb'); print(c.execute('SELECT COUNT(*) FROM raw.events').fetchone()); print(c.execute('SELECT COUNT(*) FROM raw.purchase_transactions').fetchone())\""
   ```
   Має показати приблизно `(32277,)` і `(53,)`.
8. dbt sanity-check:
   ```bash
   docker compose exec airflow-scheduler bash -lc "cd /opt/airflow/dbt && dbt debug"
   ```
   Має бути `All checks passed!`.

## `.env`

Файл не комітиться. Приклад вмісту:

```
AIRFLOW_UID=50000
```

## Що вже зроблено

- [x] Інфра: docker-compose (postgres, mysql, airflow-webserver, airflow-scheduler)
- [x] Кастомний airflow-образ з усіма залежностями (`Dockerfile`)
- [x] Airflow Connection `mysql_game_db` (створюється вручну через UI, див. вище)
- [x] dbt-проєкт ініціалізований (`dbt_project.yml`, `profiles.yml`, `dbt debug` = OK)
- [x] Скрипт `scripts/my_sql_to_duck.py` — MySQL → `raw.purchase_transactions`
- [x] Скрипт `scripts/load_json_to_duck.py` — NDJSON → `raw.events`
- [x] DAG `game_analytics_pipeline` (два таски, послідовно через `>>` щоб не було лока DuckDB)
- [x] Raw-шар у DuckDB перевірений (rows > 0)

## Що лишилось зробити (TODO)

- [ ] **Крок 9. dbt sources**
  Описати джерела в `dbt/models/raw/sources.yml` — схема `raw`, таблиці `events` і `purchase_transactions`, з базовими тестами (`not_null`, `unique` на ключах).

- [ ] **Крок 10. Staging-моделі (`dbt/models/stg/`)**
  - `stg_events.sql` — розпарсити `raw.events`, привести типи (`event_time` → TIMESTAMP), витягти ключові поля з JSON.
  - `stg_purchases.sql` — нормалізувати `raw.purchase_transactions`, каст типів, прибрати дублікати.
  - Матеріалізація: view.
  - Додати schema-тести (not_null на PK, accepted_values на event_type тощо).

- [ ] **Крок 11. Mart-моделі (`dbt/models/mart/`)**
  Приклади:
  - `dim_users.sql` — унікальні користувачі з обох джерел.
  - `fct_daily_revenue.sql` — агрегація покупок по днях/гравцях.
  - `fct_user_activity.sql` — сесії/події по користувачах.
  - Матеріалізація: table.

- [ ] **Крок 12. Інтеграція dbt в Airflow DAG**
  Додати в `game_analytics_pipeline_dag.py` таски:
  ```
  extract_mysql >> load_json >> dbt_run >> dbt_test
  ```
  через `BashOperator` (`cd /opt/airflow/dbt && dbt run` / `dbt test`) або Cosmos.

- [ ] **Крок 13. Data quality**
  - Додати `dbt test` на ключові моделі.
  - (Опційно) Great Expectations або `dbt-expectations` для більш складних перевірок.

- [ ] **Крок 14. Документація і демо**
  - `dbt docs generate && dbt docs serve` — лінеaж.
  - Короткий звіт зі скріншотами (Airflow Graph, dbt docs, приклад запиту до mart).

## Корисні команди

```bash
# рестарт тільки airflow
docker compose restart airflow-webserver airflow-scheduler

# подивитися логи scheduler
docker compose logs -f airflow-scheduler

# зайти у shell airflow-контейнера
docker compose exec airflow-scheduler bash

# dbt run/test
docker compose exec airflow-scheduler bash -lc "cd /opt/airflow/dbt && dbt run"
docker compose exec airflow-scheduler bash -lc "cd /opt/airflow/dbt && dbt test"

# зупинити все і зберегти дані
docker compose down

# повний ресет (стирає mysql/postgres томи!)
docker compose down -v
```

## Граблі на які вже наступили

- `mysqlclient` не встановлений в базовому airflow-образі → провайдер MySQL не показується в UI. Фікс: передвстановлений у `Dockerfile`.
- `git` не встановлений → `dbt debug` падає на одному check. Фікс: передвстановлений у `Dockerfile`.
- DuckDB тримає файл ексклюзивно → два Airflow-таски паралельно не можуть писати. Фікс: у DAG-у таски йдуть послідовно (`t_extract_mysql >> t_load_json`).
- Порт MySQL: **всередині** docker-мережі `3306`, **ззовні** (з хоста) `3307`. В Airflow Connection використовуй `3306` і host `mysql`.
