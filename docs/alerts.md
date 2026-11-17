# Дашборды и алерты

Пороги живут в Prometheus rules, не в экспортере: свежесть стриминговой таблицы и суточного батча разная. JSON дашбордов лежит в [deploy/grafana/dashboards/](../deploy/grafana/dashboards/), правила — в [deploy/prometheus/iceberg.rules.yaml](../deploy/prometheus/iceberg.rules.yaml). Имена серий и смысл кэша описаны в [metrics.md](metrics.md).

На всех дашбордах переменная `ref` по умолчанию `main`, чтобы feature-ветки не занимали top-N. Job target `up` называется `iceberg-exporter`.

Две recording rule должны совпадать с тем, как запущен процесс:

- `iceberg_rules:catalog_list_interval_seconds` — `vector(60)`. Для Nessie это интервал scrape: list заменён проверкой hash. Для каталога без revision сюда ставится `max(scrape interval, ListInterval)`.
- `iceberg_rules:manifest_refresh_interval_seconds` — `vector(3600)`, дефолт `metadata.DefaultRefreshInterval` (1 ч).

От них считаются `iceberg_rules:catalog_list_max_age_seconds` (три интервала) и `iceberg_rules:manifest_cache_fresh_seconds` (два интервала скана).

`iceberg_catalog_ref_head_age_seconds` на дашборде есть всегда. Алерт по нему молчит, пока для пары catalog и ref нет своей recording rule:

```yaml
- record: iceberg_rules:ref_head_max_age_seconds
  expr: (iceberg_catalog_ref_head_age_seconds{catalog="prod",ref="main"} * 0) + 3600
```

Включайте её только для ref, от которых ждут регулярные коммиты.

Коммиты за час — gauge новых hash, сброшенный на scrape. На графике `sum_over_time`, не `rate()`:

```promql
sum by (catalog, ref) (
  sum_over_time(iceberg_catalog_commits_since_scrape{catalog=~"$catalog",ref=~"$ref"}[1h])
)
```

Средний размер файла считается из snapshot summary:

```promql
iceberg_table_files_size_bytes / clamp_min(iceberg_table_data_files, 1)
```

File-level правила умножаются на возраст per-table кэша. Гистограмма и skew на каждом scrape приходят из памяти, поэтому `timestamp()` свежий скан от закэшированного не отличает.

## Каталог

Дашборд [iceberg-catalog.json](../deploy/grafana/dashboards/iceberg-catalog.json), uid `iceberg-catalog`, заголовок «Iceberg: каталог». Размеры файлов здесь не строятся.

Панели:

- `iceberg_catalog_up` по `catalog`
- возраст HEAD и `iceberg_catalog_list_age_seconds`
- коммиты за час через `sum_over_time` выше
- `iceberg_tables`, `iceberg_views`, `iceberg_namespaces`
- `iceberg_ref_tables_diverged` относительно base ref
- `iceberg_exporter_manifest_queue`
- доля таблиц с ошибкой: `count(max by (catalog, ref, namespace, table) (iceberg_table_scrape_error) == 1) / iceberg_tables`

| Алерт | Severity | For | Условие |
| --- | --- | --- | --- |
| `IcebergCatalogDown` | page | 5m | `iceberg_catalog_up == 0` |
| `IcebergExporterDown` | page | 5m | `up{job="iceberg-exporter"} == 0` |
| `IcebergCatalogListStale` | page | — | `iceberg_catalog_list_age_seconds` больше `iceberg_rules:catalog_list_max_age_seconds`. Для Nessie это застрявшая проверка hash |
| `IcebergCatalogScrapeErrors` | page | 15m | число таблиц с `iceberg_table_scrape_error == 1` больше 5% от `iceberg_tables` на том же catalog и ref |
| `IcebergManifestQueueGrowing` | warning | 30m | `deriv(iceberg_exporter_manifest_queue[15m]) > 0`. File-level панели в этот момент отстают по свежести |
| `IcebergCatalogRefHeadStale` | warning | 15m | возраст HEAD больше `iceberg_rules:ref_head_max_age_seconds` той же пары catalog и ref. Без этой recording rule алерт молчит |

`IcebergCatalogListStale` без `for`: возраст уже сравнивается с тремя интервалами.

## Здоровье таблиц

Дашборд [iceberg-table-health.json](../deploy/grafana/dashboards/iceberg-table-health.json), uid `iceberg-table-health`, заголовок «Iceberg: здоровье таблиц». Сортировка `topk(20, ...)`, не график всех серий. `histogram_quantile` на этом дашборде нет.

Панели:

- возраст snapshot: `topk(20, iceberg_table_snapshot_age_seconds)`
- records, data files, байты и их `delta(...[24h])`
- средний размер файла из формулы выше
- delete pressure: `iceberg_table_position_deletes / clamp_min(iceberg_table_records, 1)` и `iceberg_table_delete_files / clamp_min(iceberg_table_data_files, 1)`. Equality deletes — отдельная панель, универсального порога у них нет
- `iceberg_table_snapshots` и `iceberg_table_metadata_log_entries`
- `iceberg_table_manifests` по `content`
- доля таблиц с `iceberg_table_format_version` 1, 2 и 3
- число таблиц по `operation`, где `iceberg_table_last_snapshot_info == 1`
- фильтр пустых таблиц: `iceberg_table_has_snapshot == 0`. Они на панели видны и не алертятся
- `iceberg_ref_tables_diverged`. Для feature-веток расхождение ожидаемо, отдельного правила по умолчанию нет. Своё правило имеет смысл только на пару prod-ref, если такой процесс есть

| Алерт | Severity | For | Условие |
| --- | --- | --- | --- |
| `IcebergTableMetadataRetention` | warning | 1h | `iceberg_table_snapshots > 1000` или `iceberg_table_metadata_log_entries > 100`. Таблицы с `iceberg_table_has_snapshot == 0` исключены. Это признак, что `expire_snapshots` и rewrite metadata не бегут |
| `IcebergTablePositionDeletes` | warning | 24h | `iceberg_table_position_deletes / clamp_min(iceberg_table_records, 1) > 0.2` при `iceberg_table_records > 1000000`. Серия берётся из snapshot summary, возраст manifest-кэша на неё не влияет |

`iceberg_table_format_version == 1` остаётся списком на дашборде. Page-алерта на format version нет.

## Одна таблица

Дашборд [iceberg-table.json](../deploy/grafana/dashboards/iceberg-table.json), uid `iceberg-table`, заголовок «Iceberg: таблица». Переменные `exported_namespace`, `catalog`, `ref`, `namespace`, `table`. `exported_namespace` — namespace экспортера в Prometheus, `ref` здесь одно значение.

Панели текущего состояния: records, data files, байты, возраст snapshot, партиции, манифесты, последняя operation, возраст кэша манифестов.

Временные ряды: records, data files, байты, средний размер файла, position deletes и equality deletes.

File-level блок подписан как кэш:

- bar gauge bucket-ов `iceberg_table_file_size_bytes` с границами 8 MiB, 32 MiB, 128 MiB, 512 MiB, 1 GiB и `+Inf`
- `histogram_quantile(0.5, ...)` и `histogram_quantile(0.95, ...)` только при `table="$table"`
- `iceberg_table_small_files_ratio`, `iceberg_table_partition_skew_ratio`, `iceberg_table_partition_bytes_max`
- `iceberg_table_delete_file_bytes`, когда delete-манифесты включены
- `iceberg_table_manifest_cache_age_seconds` рядом с этими панелями

| Алерт | Severity | For | Условие |
| --- | --- | --- | --- |
| `IcebergTableSmallFiles` | warning | 6h | `iceberg_table_small_files_ratio > 0.5` и `iceberg_table_data_files > 100`, пока `iceberg_table_manifest_cache_age_seconds` меньше `iceberg_rules:manifest_cache_fresh_seconds`. Порог 32 MiB зашит в расчёт серии |
| `IcebergTablePartitionSkew` | warning | 1h | `iceberg_table_partition_skew_ratio > 10` и `iceberg_table_partitions >= 10` при том же свежем кэше. На двух партициях ratio шумит и в правило не входит |
| `IcebergTableManifestCacheStale` | warning | 30m | `iceberg_table_manifest_cache_age_seconds` больше двух интервалов скана. Это алерт на экспортер: гистограмма и skew в это время продолжают отдаваться из памяти |

## Настройки таблиц

Дашборд [iceberg-namespace.json](../deploy/grafana/dashboards/iceberg-namespace.json), uid `iceberg-namespace`, заголовок «Iceberg: настройки таблиц». Переменные `exported_namespace`, `catalog`, `ref` и Iceberg `namespace`. `namespace` по умолчанию All. Это список таблиц, а не график: размер, свойства записи и флаги.

Свойства читаются из metadata JSON. Пустая ячейка размера значит, что ключ не задан. Дефолт спецификации и сессионный конфиг движка на дашборд не попадают. Партиции, skew, доля мелких файлов и возраст кэша — file-level кэш.

Отдельных алертов по этим флагам нет. Цвет на дашборде:

- format version 1 — красный
- `merge-on-read` — жёлтый. Красный флаг, если при этом `position_deletes / records > 0.2` или `equality_deletes > 0`
- delete, update и merge все заданы и не совпадают — жёлтый флаг
- target file size задан и меньше 64 MiB или больше 1 GiB — красный
- row group меньше 8 MiB, больше 512 MiB или больше target file size — красный флаг
- средний файл меньше 25% target при `data_files > 100` — жёлтый
- `distribution_mode=none` при ненулевом числе полей spec — жёлтый
- `small_files_ratio > 0.5` при `data_files > 100` и skew `> 10` при `partitions >= 10` — красные флаги, те же пороги, что у warning-алертов file-level
- spec без полей и размер больше 100 GiB — жёлтый
- `iceberg_table_partitions > 10000` — жёлтый

Имя таблицы ведёт на дашборд одной таблицы.

## Чего этими правилами не поймать

Откатившийся коммит не меняет snapshot, и экспортер его не видит. Планирование запросов, число попыток коммита и пропущенные файлы при скане приходят из ScanReport и CommitReport движка. Файлы-сироты после rewrite не видны, пока нет отдельной сверки warehouse со snapshot. Успех compaction виден косвенно: падает small-file ratio и delete pressure, растёт средний размер файла.
