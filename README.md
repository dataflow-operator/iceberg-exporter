# iceberg-exporter

Prometheus pull-экспортер состояния Iceberg-таблиц. Первая реализация каталога — Nessie API v2. Каталог отдаёт указатель на таблицу, статистика читается из warehouse один раз на новую `metadataLocation` и ещё раз на новый snapshot. Другой каталог подключается тем же портом `internal/catalog` и не меняет метрики таблиц.

`GET /metrics` отдаёт последний собранный текст. Сбор текста идёт в конце фонового refresh и не блокирует HTTP. Обход каталога и чтение warehouse тоже идут в фоне.

Справочник серий — [docs/metrics.md](docs/metrics.md). Дашборды, PromQL и пороги — [docs/alerts.md](docs/alerts.md).

## Как собираются данные

Фоновый цикл `exporter.Run` с интервалом `exporter.Config.Interval` (по умолчанию 60 с) для каждого каталога делает следующее.

1. `Probe`. Ошибка ставит `iceberg_catalog_up` в 0 и оставляет прошлые серии таблиц.
2. Если адаптер реализует `Revisioned` и токен ref совпал, список таблиц не запрашивается. У Nessie токен — commit hash с `GET /api/v2/trees/{ref}`.
3. Если токен сменился и адаптер реализует `Diffable`, применяется diff. У Nessie это `GET /api/v2/trees/{ref}@{from}/diff/{ref}@{to}`. Полный список остаётся холодным стартом и сверкой по `ReconcileInterval`.
4. Без revision каждый scrape сравнивает `metadataLocation` с кэшем. `ListInterval` больше нуля отдаёт прошлый список, пока интервал не истёк. `snapshotId` проверяется только когда он есть у обеих сторон.
5. Metadata JSON читается по `metadataLocation`, если адаптер не вложил его в указатель. Nessie native API вкладывает только location и snapshot id. Кэш ключуется content id (или table-uuid, а до него — namespace и имя) и не перечитывается, пока location не сменился.
6. Manifest list текущего snapshot читается отдельно и кэшируется по content id и snapshot id. Один и тот же snapshot на разных ref не читается дважды.
7. Manifest entries не стоят на пути scrape. Если file-level кэш старше `manifest_refresh_interval`, refresh ставит snapshot в очередь, когда собирает текст метрик. Воркеры читают data-файлы со status `added` и `existing`. Delete-манифесты читаются только при `DeleteManifests`.

Views попадают в `iceberg_views` и в file-level скан не идут. JVM-метрики Nessie (`/q/metrics`) экспортер не читает.

## Чего экспортер не видит

Откатившийся коммит, который не сменил snapshot, в сериях не появляется. Планы запросов, число попыток коммита и пропущенные при скане файлы приходят из ScanReport и CommitReport движка. Файлы-сироты после rewrite видны только отдельной сверкой warehouse со snapshot. Успех compaction виден косвенно: падает доля мелких файлов и давление delete, растёт средний размер файла из snapshot summary.

## Сборка и запуск

Отдельного `cmd` в модуле нет. Процесс собирается из библиотеки: клиент Nessie, `metadata.Collector` с реализацией `metadata.Reader` и `exporter.Exporter`. Go 1.25.

```go
client, err := nessie.New(nessie.Config{
    Name:    "prod",
    BaseURL: os.Getenv("NESSIE_URL"),
    Token:   os.Getenv("NESSIE_TOKEN"),
    Refs:    []catalog.Ref{{Name: "main", Kind: catalog.RefBranch}},
    BaseRef: "main",
})
meta := metadata.NewCollector(reader, metadata.WithEntries(metadata.EntriesConfig{}))
exp, err := exporter.New(exporter.Config{}, meta, client)
go exp.Run(ctx)
http.Handle("/metrics", exp.Handler())
```

`metadata.Reader` читает объекты warehouse по location из каталога. Схема (`s3`, `gs`, `abfs`, `file`) выбирает вызывающий код. В метрики и логи не попадают токен и query URI. `BaseURL` Nessie не должен содержать userinfo и query: токен передаётся полем `Token` и уходит заголовком `Authorization: Bearer`. Пустой токен означает запрос без заголовка. При редиректе на другой host заголовок снимается.

Имя job в Prometheus должно быть `iceberg-exporter`: на него завязан алерт `IcebergExporterDown`.

```yaml
scrape_configs:
  - job_name: iceberg-exporter
    scrape_interval: 60s
    metrics_path: /metrics
    static_configs:
      - targets: ["exporter:8080"]
```

`scrape_interval` в примере совпадает с recording rule `iceberg_rules:catalog_list_interval_seconds` (`vector(60)` в [deploy/prometheus/iceberg.rules.yaml](deploy/prometheus/iceberg.rules.yaml)). Для Nessie list заменён проверкой hash, поэтому в правило подставляется интервал scrape, а не `ListInterval`.

## Конфигурация

Файла конфигурации нет. Ноль в поле длительности или размера включает дефолт из кода, кроме интервалов списка: нулевой `ListInterval` для каталога без revision означает list на каждом refresh, нулевой `ReconcileInterval` отключает полную сверку.

| Поле | Где | По умолчанию | Смысл |
| --- | --- | --- | --- |
| `Name` | `nessie.Config` | обязательно | Label `catalog`. Два Nessie на одном экспортере различаются этим именем, не типом адаптера |
| `BaseURL` | `nessie.Config` | обязательно | Origin Nessie, суффикс `/api/v2` необязателен |
| `Token` | `nessie.Config` | пусто | Bearer-секрет. Передавайте из env |
| `Refs` | `nessie.Config` | обязательно | Ветки и теги, которые обходятся. Пустой kind — branch |
| `BaseRef` | `nessie.Config` | пусто | База для `iceberg_ref_tables_diverged`. Пустое значение отключает сравнение. Если задано, ref должен быть в `Refs` |
| `PageSize` | `nessie.Config`, `exporter.Config` | 0 | `max-records` для entries и diff. 0 не передаёт параметр. Отрицательное значение отвергается |
| `HistoryMaxRecords` | `nessie.Config` | 100 | `max-records` журнала коммитов. Читает только адаптер с `Historian` |
| `Interval` | `exporter.Config` | 60 с | Период `Probe` и refresh. Scrape его не ждёт |
| `ListInterval` | `exporter.Config` | 0, list каждый refresh | Для каталога без revision: сколько отдавать кэш списка. Для Nessie не используется |
| `ReconcileInterval` | `exporter.Config` | 0, сверка выключена | Полный list даже при совпавшем hash, в том числе у Nessie |
| `Namespaces` | `exporter.Config` | все | Include и exclude. Exclude сильнее. Шаблон `analytics` покрывает `analytics` и `analytics.daily` |
| `RefreshInterval` | `metadata.EntriesConfig` | 1 ч | Сколько переиспользовать file-level кэш. Snapshot неизменяем, интервал ограничивает повтор после ошибки |
| `Concurrency` | `metadata.EntriesConfig` | 2 | Пул чтения manifest entries. Metadata и manifest list в эту очередь не встают |
| `MaxQueue` | `metadata.EntriesConfig` | 256 | Сколько snapshot ждут скана |
| `MaxBytes` | `metadata.EntriesConfig` | 512 MiB | Бюджет warehouse на один snapshot |
| `SmallFileBytes` | `metadata.EntriesConfig` | 32 MiB | Файл строго меньше порога считается мелким. Порог попадает в HELP `iceberg_table_small_files_ratio`, не в label |
| `DeleteManifests` | `metadata.EntriesConfig` | false | Читать delete-манифесты и публиковать `iceberg_table_delete_file_bytes` |

Без `WithEntries` file-level серии не публикуются. `iceberg_exporter_manifest_queue` при этом остаётся и равен нулю.

## Куда смотреть

- `/metrics` — кэш каталога и таблиц
- [docs/metrics.md](docs/metrics.md) — имя, тип, labels и источник каждой серии
- [docs/alerts.md](docs/alerts.md) — четыре дашборда и правила
- [deploy/grafana/dashboards/](deploy/grafana/dashboards/) — JSON: каталог, здоровье таблиц, одна таблица, настройки таблиц
- [deploy/prometheus/iceberg.rules.yaml](deploy/prometheus/iceberg.rules.yaml) — recording rules и алерты

Переменная `ref` на дашбордах по умолчанию `main`.

## Локальный стенд

`task up` поднимает Nessie, MinIO, генератор таблиц, экспортёр, Prometheus и Grafana. Генератор пишет `analytics.events`, `analytics.orders`, пустую `ops.empty`, view и ветку `dev`, затем 5 минут раз в 20 секунд добавляет строки в `main` и останавливается. PyIceberg 0.10 удаляет строки перезаписью файла, поэтому `iceberg_table_position_deletes` остаётся 0, а последняя операция `orders` — `overwrite`.

- Grafana: http://localhost:3001 (анонимный Admin, дашборды в папке Iceberg)
- Prometheus: http://localhost:9090
- Метрики: http://localhost:8080/metrics
- MinIO: http://localhost:9003 (API на порту 9002)

`task check` ждёт `iceberg_catalog_up` и ненулевое число таблиц. `task down` останавливает контейнеры, `task reset` удаляет volume MinIO. Нужны Docker и [go-task](https://taskfile.dev).

## Тесты и пакеты

```sh
go test ./...
```

- `internal/catalog` — порт, кэш списка, diff и расхождение snapshot с base ref
- `internal/nessie` — клиент Nessie API v2
- `internal/metadata` — metadata JSON, manifest list и агрегация entries
- `internal/exporter` — фоновый refresh и `GET /metrics`
- `deploy/monitoring` — сверка rules, дашбордов и этих страниц с именами серий
