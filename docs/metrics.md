# Метрики

Префикс `iceberg_`. Все серии — gauge, кроме гистограммы `iceberg_table_file_size_bytes`. Текст HELP на `/metrics` английский и совпадает с описаниями ниже.

Labels таблиц: `catalog`, `ref`, `namespace`, `table`. `catalog` — имя из конфигурации адаптера. `type` есть только у `iceberg_catalog_up` и равен типу адаптера (`nessie`). Content id, snapshot id, путь metadata и текст коммита в labels не входят.

Серия capability публикуется, когда адаптер эту capability реализует и значение уже известно. Пока серии нет, каталог эту величину не отдал. Ноль публикуется отдельно и означает измеренный ноль.

`GET /metrics` читает память. Возраст snapshot и возраст кэшей считается в момент scrape.

## Каталог

Источник — последний успешный обход. Ошибка `Probe` ставит `iceberg_catalog_up` в 0 и сохраняет прошлые серии таблиц и refs.

| Серия | Labels | Когда появляется | Значение |
| --- | --- | --- | --- |
| `iceberg_catalog_up` | `catalog`, `type` | после первого `Probe` | 1, если последний probe успешен |
| `iceberg_tables` | `catalog`, `ref` | ref есть в кэше | число Iceberg-таблиц на ref, без views |
| `iceberg_views` | `catalog`, `ref` | ref есть в кэше | число views на ref |
| `iceberg_namespaces` | `catalog`, `ref` | ref есть в кэше | число разных namespace среди таблиц и views |
| `iceberg_catalog_list_age_seconds` | `catalog`, `ref` | известна метка времени кэша | возраст кэша списка. У `Revisioned` (Nessie) это возраст последней успешной проверки hash. У остальных — возраст последнего полного list |

Серии ниже есть только у заявленной capability.

| Серия | Capability | Labels | Значение |
| --- | --- | --- | --- |
| `iceberg_catalog_ref_head_age_seconds` | время HEAD известно | `catalog`, `ref` | возраст commit time HEAD. У Nessie время берётся из журнала `GET /api/v2/trees/{ref}/history`, не из `GET /trees/{ref}` |
| `iceberg_catalog_commits_since_scrape` | `Historian` | `catalog`, `ref` | gauge новых токенов с прошлого scrape этой серии. Scrape читает значение и сбрасывает его. Для графика коммитов используйте `sum_over_time(...[1h])`. `rate()` эту серию не описывает. Если прошлый hash в прочитанном журнале не встретился, gauge содержит число прочитанных коммитов. Первый успешный обход с пустым известным hash публикует 0 |
| `iceberg_ref_tables_diverged` | `MultiRef` и задан base ref | `catalog`, `ref`, `base_ref` | таблицы ref, чей snapshot id отличается от base, плюс таблицы, которые есть только на ref. Views не считаются. Таблицы, которые есть только на base, в серию ref не входят. Сравнение идёт по content id, затем по namespace и имени |

## Metadata JSON

Источник — текущий snapshot summary и логи metadata. Файл читается, когда указатель не содержит JSON и пара content id + `metadataLocation` ещё не в кэше. Пустая таблица без current snapshot валидна: `iceberg_table_has_snapshot` равен 0, счётчики summary равны 0, `iceberg_table_last_snapshot_info` отсутствует.

Ошибка одной таблицы ставит `iceberg_table_scrape_error` и не снимает остальные таблицы. Если у таблицы уже был успешный разбор, при следующей ошибке metadata остаются прошлые gauge и `stage="metadata"` становится 1.

| Серия | Значение |
| --- | --- |
| `iceberg_table_has_snapshot` | 1, если есть текущий snapshot |
| `iceberg_table_format_version` | format version из metadata JSON |
| `iceberg_table_snapshot_age_seconds` | возраст текущего snapshot на момент scrape. 0 без snapshot или без времени snapshot |
| `iceberg_table_last_updated_timestamp_seconds` | unix-время `last-updated-ms`. 0, если время нулевое |
| `iceberg_table_snapshots` | число snapshot в metadata |
| `iceberg_table_metadata_log_entries` | число предыдущих metadata-файлов в metadata log |
| `iceberg_table_snapshot_log_entries` | число записей snapshot log |
| `iceberg_table_records` | `total-records` |
| `iceberg_table_data_files` | `total-data-files` |
| `iceberg_table_files_size_bytes` | `total-files-size` |
| `iceberg_table_delete_files` | `total-delete-files` |
| `iceberg_table_position_deletes` | `total-position-deletes` |
| `iceberg_table_equality_deletes` | `total-equality-deletes` |
| `iceberg_table_last_snapshot_added_data_files` | `added-data-files` текущего snapshot |
| `iceberg_table_last_snapshot_added_records` | `added-records` |
| `iceberg_table_last_snapshot_added_files_size_bytes` | `added-files-size` |
| `iceberg_table_last_snapshot_info` | 1 для operation текущего snapshot. Дополнительный label `operation`: `append`, `overwrite`, `delete`, `replace` |
| `iceberg_table_scrape_error` | 1, если последнее чтение таблицы упало на этом этапе. Дополнительный label `stage` |

`stage` принимает три значения, и каждое появляется отдельно:

- `metadata` — публикуется для каждой таблицы в кэше. 1 означает ошибку чтения или разбора metadata JSON.
- `manifest_list` — публикуется после успешного metadata или когда упал только manifest list. Падение list сохраняет gauge metadata.
- `manifest_entries` — публикуется после попытки file-level скана. Падение entries сохраняет gauge metadata и manifest list.

Средний размер data file считается из summary и не ждёт манифесты:

```promql
iceberg_table_files_size_bytes / clamp_min(iceberg_table_data_files, 1)
```

## Manifest list

Один Avro на snapshot. Пока list не прочитан, `iceberg_table_manifests` и `iceberg_table_partition_specs` отсутствуют, gauge metadata уже есть. У таблицы без snapshot обе серии публикуются нулями.

| Серия | Labels сверх табличных | Значение |
| --- | --- | --- |
| `iceberg_table_manifests` | `content` = `data` или `deletes` | число манифестов текущего snapshot |
| `iceberg_table_partition_specs` | — | число разных partition spec в manifest list. Пути без spec id считаются по spec из metadata. 0 без snapshot |

## Manifest entries

Медленный контур. Ключ кэша — content id и snapshot id. `/metrics` на каждом scrape перепубликует серии из памяти, поэтому `timestamp()` не отличает свежий скан от закэшированного. Рядом с file-level панелью смотрите `iceberg_table_manifest_cache_age_seconds`.

Сканируются живые data-файлы: status `added` и `existing`. Status `deleted` в агрегаты не входит. Серии появляются после первого успешного скана snapshot. Таблица без snapshot и snapshot без файлов для скана получают нулевой агрегат сразу, без чтения Avro, и их `iceberg_table_manifest_cache_age_seconds` обновляется на refresh. Без `WithEntries` file-level серий нет.

| Серия | Тип | Значение |
| --- | --- | --- |
| `iceberg_table_file_size_bytes` | histogram | размер живых data files. Верхние границы bucket: 8 MiB, 32 MiB, 128 MiB, 512 MiB, 1 GiB (`le` 8388608, 33554432, 134217728, 536870912, 1073741824 и `+Inf`). Prometheus добавляет `_bucket`, `_sum`, `_count` |
| `iceberg_table_small_files_ratio` | gauge | доля живых data files строго меньше порога. Порог в HELP, по умолчанию 33554432 байт. Файл ровно этого размера мелким не считается |
| `iceberg_table_partitions` | gauge | число разных партиций среди живых data files. Значения партиций в labels не входят |
| `iceberg_table_partition_bytes_max` | gauge | максимум байт одной партиции |
| `iceberg_table_partition_skew_ratio` | gauge | max / среднее по байтам партиций. 0, если у snapshot нет байт data files |
| `iceberg_table_delete_file_bytes` | gauge | байты живых delete-файлов. Label `content`: `position` или `equality`. Серия есть, когда `DeleteManifests` включён и скан успешен. Счётчики delete в snapshot summary от этого скана не зависят |
| `iceberg_table_manifest_cache_age_seconds` | gauge | секунды с момента расчёта file-level кэша этой таблицы. Публикуется вместе с успешным агрегатом |

`histogram_quantile` по всем таблицам сразу строит ряд на каждую пару bucket и таблица. Считайте квантиль для одной выбранной таблицы.

## Очередь экспортера

`iceberg_exporter_manifest_queue` — gauge без labels. Это число snapshot, которые ждут чтения manifest entries. File-level метрики таблиц при ненулевой очереди продолжают отдаваться из памяти. Длина очереди ограничена `MaxQueue` (по умолчанию 256). Snapshot сверх лимита остаётся без нового скана до следующего размещения.
