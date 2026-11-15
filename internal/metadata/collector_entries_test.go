package metadata

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"iceberg-exporter/internal/catalog"
)

const (
	entryDataPath   = "s3://wh/db/events/metadata/data-0.avro"
	entryDataPathB  = "s3://wh/db/events/metadata/data-1.avro"
	entryDeletePath = "s3://wh/db/events/metadata/deletes.avro"
)

func TestUpdateServesCachedFileMetrics(t *testing.T) {
	t.Parallel()
	body := partitionedManifest(t)
	reader := &memReader{files: map[string][]byte{
		currentManifestList: manifestListBytes(t, 7, []iceberg.ManifestFile{
			manifestFile(t, entryDataPath, iceberg.ManifestContentData, 0),
		}),
		entryDataPath: body,
	}}
	now := time.Unix(1_800_000_000, 0).UTC()
	c := syncCollector(t, reader, EntriesConfig{SmallFileBytes: 500})
	c.now = func() time.Time { return now }
	if err := c.Update(context.Background(), []Table{entryTable("main", "v1.metadata.json", 7, nil)}); err != nil {
		t.Fatalf("update: %v", err)
	}
	before := samples(t, c)
	prefix := `catalog=prod,namespace=db,ref=main,table=events`
	requireGauge(t, before, "iceberg_table_records{"+prefix+"}", 10)
	if _, ok := before["iceberg_table_small_files_ratio{"+prefix+"}"]; ok {
		t.Fatal("file metrics were published before the entry scan")
	}
	requireGauge(t, before, "iceberg_exporter_manifest_queue{}", 1)

	if err := c.WaitEntries(context.Background()); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if got := reader.ReadsAt(entryDataPath); got != 1 {
		t.Fatalf("data manifest reads = %d, want 1", got)
	}
	got := samples(t, c)
	requireGauge(t, got, "iceberg_table_small_files_ratio{"+prefix+"}", 2.0/3.0)
	requireGauge(t, got, "iceberg_table_partitions{"+prefix+"}", 2)
	requireGauge(t, got, "iceberg_table_partition_bytes_max{"+prefix+"}", 1000)
	requireGauge(t, got, "iceberg_table_partition_skew_ratio{"+prefix+"}", float64(1000)*2/1400)
	requireGauge(t, got, "iceberg_table_manifest_cache_age_seconds{"+prefix+"}", 0)
	requireGauge(t, got, "iceberg_table_scrape_error{catalog=prod,namespace=db,ref=main,stage=manifest_entries,table=events}", 0)
	requireGauge(t, got, "iceberg_exporter_manifest_queue{}", 0)
	if _, ok := got["iceberg_table_delete_file_bytes{catalog=prod,content=position,namespace=db,ref=main,table=events}"]; ok {
		t.Fatal("delete bytes were published without delete manifests enabled")
	}
	hist := histograms(t, c)
	h := hist["iceberg_table_file_size_bytes{"+prefix+"}"]
	if h.count != 3 || h.sum != 1400 {
		t.Fatalf("histogram count=%d sum=%v", h.count, h.sum)
	}
	for _, bound := range fileSizeBucketBounds {
		if h.buckets[bound] != 3 {
			t.Fatalf("bucket %v = %d, want 3 in %v", bound, h.buckets[bound], h.buckets)
		}
	}
	for _, name := range h.labels {
		if name == "partition" || strings.Contains(name, "partition") {
			t.Fatalf("partition label %q", name)
		}
	}
	help := helpText(t, c, "iceberg_table_small_files_ratio")
	if !strings.Contains(help, "500") {
		t.Fatalf("help %q does not name the threshold", help)
	}
	if !strings.Contains(helpText(t, c, "iceberg_table_file_size_bytes"), "cache") {
		t.Fatal("histogram help does not say the series is cached")
	}

	now = now.Add(30 * time.Second)
	requireGauge(t, samples(t, c), "iceberg_table_manifest_cache_age_seconds{"+prefix+"}", 30)
	if got := reader.ReadsAt(entryDataPath); got != 1 {
		t.Fatalf("collect reread the manifest %d times", got)
	}
}

func TestEntryScanIsSharedAcrossRefs(t *testing.T) {
	t.Parallel()
	reader := &memReader{files: map[string][]byte{
		currentManifestList: manifestListBytes(t, 7, []iceberg.ManifestFile{
			manifestFile(t, entryDataPath, iceberg.ManifestContentData, 0),
		}),
		entryDataPath: partitionedManifest(t),
	}}
	c := syncCollector(t, reader, EntriesConfig{})
	tables := []Table{
		entryTable("main", "v1.metadata.json", 7, nil),
		entryTable("dev", "v1.metadata.json", 7, nil),
	}
	if err := c.Update(context.Background(), tables); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := c.WaitEntries(context.Background()); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if got := reader.ReadsAt(entryDataPath); got != 1 {
		t.Fatalf("shared snapshot reads = %d, want 1", got)
	}
	got := samples(t, c)
	requireGauge(t, got, "iceberg_table_partitions{catalog=prod,namespace=db,ref=main,table=events}", 2)
	requireGauge(t, got, "iceberg_table_partitions{catalog=prod,namespace=db,ref=dev,table=events}", 2)
}

func TestEntryScanRefreshesOnIntervalAndNewSnapshot(t *testing.T) {
	t.Parallel()
	nextList := "s3://wh/db/events/metadata/snap-8.avro"
	reader := &memReader{files: map[string][]byte{
		currentManifestList: manifestListBytes(t, 7, []iceberg.ManifestFile{
			manifestFile(t, entryDataPath, iceberg.ManifestContentData, 0),
		}),
		nextList: manifestListBytes(t, 8, []iceberg.ManifestFile{
			manifestFile(t, entryDataPathB, iceberg.ManifestContentData, 0),
		}),
		entryDataPath:  partitionedManifest(t),
		entryDataPathB: partitionedManifest(t),
	}}
	now := time.Unix(1_800_000_000, 0).UTC()
	c := syncCollector(t, reader, EntriesConfig{RefreshInterval: time.Minute})
	c.now = func() time.Time { return now }
	if err := c.Update(context.Background(), []Table{entryTable("main", "v1.metadata.json", 7, nil)}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := c.WaitEntries(context.Background()); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if err := c.Update(context.Background(), []Table{entryTable("main", "v1.metadata.json", 7, nil)}); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if err := c.WaitEntries(context.Background()); err != nil {
		t.Fatalf("wait refresh: %v", err)
	}
	if got := reader.ReadsAt(entryDataPath); got != 1 {
		t.Fatalf("fresh cache reread the manifest %d times", got)
	}

	now = now.Add(time.Minute)
	if err := c.Update(context.Background(), []Table{entryTable("main", "v1.metadata.json", 7, nil)}); err != nil {
		t.Fatalf("stale update: %v", err)
	}
	if err := c.WaitEntries(context.Background()); err != nil {
		t.Fatalf("stale wait: %v", err)
	}
	if got := reader.ReadsAt(entryDataPath); got != 2 {
		t.Fatalf("stale cache reads = %d, want 2", got)
	}

	if err := c.Update(context.Background(), []Table{entryTable("main", "v2.metadata.json", 8, &nextList)}); err != nil {
		t.Fatalf("new snapshot: %v", err)
	}
	if err := c.WaitEntries(context.Background()); err != nil {
		t.Fatalf("new snapshot wait: %v", err)
	}
	if got := reader.ReadsAt(entryDataPathB); got != 1 {
		t.Fatalf("new snapshot reads = %d, want 1", got)
	}
}

func TestEntryRefreshFailureKeepsLastAggregate(t *testing.T) {
	t.Parallel()
	reader := &memReader{files: map[string][]byte{
		currentManifestList: manifestListBytes(t, 7, []iceberg.ManifestFile{
			manifestFile(t, entryDataPath, iceberg.ManifestContentData, 0),
		}),
		entryDataPath: partitionedManifest(t),
	}}
	now := time.Unix(1_800_000_000, 0).UTC()
	c := syncCollector(t, reader, EntriesConfig{RefreshInterval: time.Minute})
	c.now = func() time.Time { return now }
	if err := c.Update(context.Background(), []Table{entryTable("main", "v1.metadata.json", 7, nil)}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := c.WaitEntries(context.Background()); err != nil {
		t.Fatalf("wait: %v", err)
	}
	reader.errAt = map[string]error{entryDataPath: errors.New("warehouse down")}
	now = now.Add(time.Minute)
	if err := c.Update(context.Background(), []Table{entryTable("main", "v1.metadata.json", 7, nil)}); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if err := c.WaitEntries(context.Background()); err != nil {
		t.Fatalf("refresh wait: %v", err)
	}
	prefix := "catalog=prod,namespace=db,ref=main,table=events"
	got := samples(t, c)
	requireGauge(t, got, "iceberg_table_partitions{"+prefix+"}", 2)
	requireGauge(t, got, "iceberg_table_records{"+prefix+"}", 10)
	requireGauge(t, got, "iceberg_table_scrape_error{catalog=prod,namespace=db,ref=main,stage=manifest_entries,table=events}", 1)
	requireGauge(t, got, "iceberg_table_manifest_cache_age_seconds{"+prefix+"}", 60)
}

func TestEmbeddedManifestEntriesAreAggregated(t *testing.T) {
	t.Parallel()
	path := "s3://wh/db/events/metadata/a.avro"
	reader := &memReader{files: map[string][]byte{path: partitionedManifest(t)}}
	c := syncCollector(t, reader, EntriesConfig{SmallFileBytes: 500})
	raw := []byte(`{
		"format-version": 2,
		"table-uuid": "` + tableUUID + `",
		"location": "s3://wh/db/events",
		"last-sequence-number": 1,
		"last-updated-ms": 1700000000000,
		"last-column-id": 1,
		"schemas": [{
			"schema-id": 0,
			"type": "struct",
			"fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]
		}],
		"current-schema-id": 0,
		"partition-specs": [{"spec-id": 0, "fields": [{"name": "id", "transform": "identity", "source-id": 1, "field-id": 1000}]}],
		"default-spec-id": 0,
		"last-partition-id": 1000,
		"properties": {},
		"sort-orders": [{"order-id": 0, "fields": []}],
		"default-sort-order-id": 0,
		"current-snapshot-id": 7,
		"snapshots": [{
			"snapshot-id": 7,
			"sequence-number": 1,
			"timestamp-ms": 1700000000000,
			"manifests": ["` + path + `"],
			"summary": {"operation": "append", "total-records": "3", "added-data-files": "1", "added-records": "3", "added-files-size": "10"}
		}],
		"snapshot-log": [{"snapshot-id": 7, "timestamp-ms": 1700000000000}]
	}`)
	if err := c.Update(context.Background(), []Table{{
		Catalog: "prod", Ref: "main",
		Pointer: catalog.TablePointer{
			ContentID: "c", Namespace: "db", Name: "events",
			MetadataLocation: "s3://wh/db/events/metadata/v1.metadata.json",
			Metadata:         raw,
		},
	}}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := c.WaitEntries(context.Background()); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if got := reader.ReadsAt(path); got != 1 {
		t.Fatalf("embedded manifest reads = %d, want 1", got)
	}
	requireGauge(t, samples(t, c), "iceberg_table_partitions{catalog=prod,namespace=db,ref=main,table=events}", 2)
	requireGauge(t, samples(t, c), "iceberg_table_manifests{catalog=prod,content=data,namespace=db,ref=main,table=events}", 1)
}

func TestEntryScanFailureKeepsMetadata(t *testing.T) {
	t.Parallel()
	reader := &memReader{files: map[string][]byte{
		currentManifestList: manifestListBytes(t, 7, []iceberg.ManifestFile{
			manifestFile(t, entryDataPath, iceberg.ManifestContentData, 0),
		}),
	}}
	now := time.Unix(1_800_000_000, 0).UTC()
	c := syncCollector(t, reader, EntriesConfig{RefreshInterval: time.Minute})
	c.now = func() time.Time { return now }
	if err := c.Update(context.Background(), []Table{entryTable("main", "v1.metadata.json", 7, nil)}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := c.WaitEntries(context.Background()); err != nil {
		t.Fatalf("wait: %v", err)
	}
	prefix := "catalog=prod,namespace=db,ref=main,table=events"
	got := samples(t, c)
	requireGauge(t, got, "iceberg_table_records{"+prefix+"}", 10)
	requireGauge(t, got, "iceberg_table_scrape_error{catalog=prod,namespace=db,ref=main,stage=manifest_entries,table=events}", 1)
	if _, ok := got["iceberg_table_partitions{"+prefix+"}"]; ok {
		t.Fatal("partial file metrics were published")
	}

	if err := c.Update(context.Background(), []Table{entryTable("main", "v1.metadata.json", 7, nil)}); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if err := c.WaitEntries(context.Background()); err != nil {
		t.Fatalf("retry wait: %v", err)
	}
	if got := reader.ReadsAt(entryDataPath); got != 1 {
		t.Fatalf("retry inside the interval read the warehouse %d times", got)
	}

	reader.files[entryDataPath] = partitionedManifest(t)
	now = now.Add(time.Minute)
	if err := c.Update(context.Background(), []Table{entryTable("main", "v1.metadata.json", 7, nil)}); err != nil {
		t.Fatalf("recovered update: %v", err)
	}
	if err := c.WaitEntries(context.Background()); err != nil {
		t.Fatalf("recovered wait: %v", err)
	}
	got = samples(t, c)
	requireGauge(t, got, "iceberg_table_partitions{"+prefix+"}", 2)
	requireGauge(t, got, "iceberg_table_scrape_error{catalog=prod,namespace=db,ref=main,stage=manifest_entries,table=events}", 0)
}

func TestQueueStaleDoesNotReadWarehouse(t *testing.T) {
	t.Parallel()
	reader := &memReader{files: map[string][]byte{
		currentManifestList: manifestListBytes(t, 7, []iceberg.ManifestFile{
			manifestFile(t, entryDataPath, iceberg.ManifestContentData, 0),
		}),
		entryDataPath: partitionedManifest(t),
	}}
	now := time.Unix(1_800_000_000, 0).UTC()
	c := syncCollector(t, reader, EntriesConfig{RefreshInterval: time.Minute, SmallFileBytes: 500})
	c.now = func() time.Time { return now }
	if err := c.Update(context.Background(), []Table{entryTable("main", "v1.metadata.json", 7, nil)}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := c.WaitEntries(context.Background()); err != nil {
		t.Fatalf("wait: %v", err)
	}
	prefix := "catalog=prod,namespace=db,ref=main,table=events"
	requireGauge(t, samples(t, c), "iceberg_exporter_manifest_queue{}", 0)
	requireGauge(t, samples(t, c), "iceberg_table_small_files_ratio{"+prefix+"}", 2.0/3.0)

	c.QueueStale(now)
	if got := reader.ReadsAt(entryDataPath); got != 1 {
		t.Fatalf("queue inside the interval read the manifest %d times", got)
	}
	requireGauge(t, samples(t, c), "iceberg_exporter_manifest_queue{}", 0)

	now = now.Add(time.Minute)
	c.QueueStale(now)
	c.QueueStale(now)
	if got := reader.ReadsAt(entryDataPath); got != 1 {
		t.Fatalf("stale queue read the manifest %d times", got)
	}
	got := samples(t, c)
	requireGauge(t, got, "iceberg_exporter_manifest_queue{}", 1)
	requireGauge(t, got, "iceberg_table_small_files_ratio{"+prefix+"}", 2.0/3.0)
	requireGauge(t, got, "iceberg_table_manifest_cache_age_seconds{"+prefix+"}", 60)
}

func TestEntryScanSkipsManifestLargerThanBudget(t *testing.T) {
	t.Parallel()
	reader := &memReader{files: map[string][]byte{
		currentManifestList: manifestListBytes(t, 7, []iceberg.ManifestFile{
			manifestFileSized(t, entryDataPath, iceberg.ManifestContentData, 0, 1<<20),
		}),
		entryDataPath: partitionedManifest(t),
	}}
	c := syncCollector(t, reader, EntriesConfig{MaxBytes: 64})
	if err := c.Update(context.Background(), []Table{entryTable("main", "v1.metadata.json", 7, nil)}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := c.WaitEntries(context.Background()); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if got := reader.ReadsAt(entryDataPath); got != 0 {
		t.Fatalf("oversized manifest was read %d times", got)
	}
	requireGauge(t, samples(t, c), "iceberg_table_scrape_error{catalog=prod,namespace=db,ref=main,stage=manifest_entries,table=events}", 1)
}

func TestEntryScanStopsAtMaxBytes(t *testing.T) {
	t.Parallel()
	reader := &memReader{files: map[string][]byte{
		currentManifestList: manifestListBytes(t, 7, []iceberg.ManifestFile{
			manifestFileSized(t, entryDataPath, iceberg.ManifestContentData, 0, 1),
		}),
		entryDataPath: partitionedManifest(t),
	}}
	c := syncCollector(t, reader, EntriesConfig{MaxBytes: 64})
	if err := c.Update(context.Background(), []Table{entryTable("main", "v1.metadata.json", 7, nil)}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := c.WaitEntries(context.Background()); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if got := reader.ReadsAt(entryDataPath); got != 1 {
		t.Fatalf("reads = %d, want the file read and then rejected", got)
	}
	prefix := "catalog=prod,namespace=db,ref=main,table=events"
	got := samples(t, c)
	requireGauge(t, got, "iceberg_table_records{"+prefix+"}", 10)
	requireGauge(t, got, "iceberg_table_scrape_error{catalog=prod,namespace=db,ref=main,stage=manifest_entries,table=events}", 1)
	if _, ok := got["iceberg_table_small_files_ratio{"+prefix+"}"]; ok {
		t.Fatal("over-budget scan published file metrics")
	}
}

func TestEntryQueueLimitDefersExtraSnapshots(t *testing.T) {
	t.Parallel()
	listA := "s3://wh/a/metadata/snap-7.avro"
	listB := "s3://wh/b/metadata/snap-7.avro"
	dataA := "s3://wh/a/metadata/data.avro"
	dataB := "s3://wh/b/metadata/data.avro"
	reader := &memReader{files: map[string][]byte{
		listA: manifestListBytes(t, 7, []iceberg.ManifestFile{
			manifestFile(t, dataA, iceberg.ManifestContentData, 0),
		}),
		listB: manifestListBytes(t, 7, []iceberg.ManifestFile{
			manifestFile(t, dataB, iceberg.ManifestContentData, 0),
		}),
		dataA: partitionedManifest(t),
		dataB: partitionedManifest(t),
	}}
	c := syncCollector(t, reader, EntriesConfig{MaxQueue: 1})
	tables := []Table{
		entryTableAt("db", "one", "a", listA, 7),
		entryTableAt("db", "two", "b", listB, 7),
	}
	if err := c.Update(context.Background(), tables); err != nil {
		t.Fatalf("update: %v", err)
	}
	requireGauge(t, samples(t, c), "iceberg_exporter_manifest_queue{}", 1)
	if err := c.WaitEntries(context.Background()); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if reader.ReadsAt(dataA)+reader.ReadsAt(dataB) != 1 {
		t.Fatalf("scans = %d+%d, want 1", reader.ReadsAt(dataA), reader.ReadsAt(dataB))
	}
	if err := c.Update(context.Background(), tables); err != nil {
		t.Fatalf("second update: %v", err)
	}
	if err := c.WaitEntries(context.Background()); err != nil {
		t.Fatalf("second wait: %v", err)
	}
	if reader.ReadsAt(dataA) != 1 || reader.ReadsAt(dataB) != 1 {
		t.Fatalf("after drain reads a=%d b=%d", reader.ReadsAt(dataA), reader.ReadsAt(dataB))
	}
}

func TestDeleteManifestBytes(t *testing.T) {
	t.Parallel()
	deletes := deleteManifest(t)
	reader := &memReader{files: map[string][]byte{
		currentManifestList: manifestListBytes(t, 7, []iceberg.ManifestFile{
			manifestFile(t, entryDataPath, iceberg.ManifestContentData, 0),
			manifestFile(t, entryDeletePath, iceberg.ManifestContentDeletes, 0),
		}),
		entryDataPath:   partitionedManifest(t),
		entryDeletePath: deletes,
	}}
	off := syncCollector(t, reader, EntriesConfig{})
	if err := off.Update(context.Background(), []Table{entryTable("main", "v1.metadata.json", 7, nil)}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := off.WaitEntries(context.Background()); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if got := reader.ReadsAt(entryDeletePath); got != 0 {
		t.Fatalf("delete manifest reads = %d, want 0", got)
	}

	reader.byLocation = nil
	reader.reads = 0
	on := syncCollector(t, reader, EntriesConfig{DeleteManifests: true})
	if err := on.Update(context.Background(), []Table{entryTable("main", "v1.metadata.json", 7, nil)}); err != nil {
		t.Fatalf("delete update: %v", err)
	}
	if err := on.WaitEntries(context.Background()); err != nil {
		t.Fatalf("delete wait: %v", err)
	}
	if got := reader.ReadsAt(entryDeletePath); got != 1 {
		t.Fatalf("delete manifest reads = %d, want 1", got)
	}
	got := samples(t, on)
	requireGauge(t, got, "iceberg_table_delete_file_bytes{catalog=prod,content=position,namespace=db,ref=main,table=events}", 10)
	requireGauge(t, got, "iceberg_table_delete_file_bytes{catalog=prod,content=equality,namespace=db,ref=main,table=events}", 25)
	requireGauge(t, got, "iceberg_table_partitions{catalog=prod,namespace=db,ref=main,table=events}", 2)
}

func TestEmptyTablePublishesZeroFileMetrics(t *testing.T) {
	t.Parallel()
	c := syncCollector(t, &memReader{}, EntriesConfig{DeleteManifests: true})
	if err := c.Update(context.Background(), []Table{{
		Catalog: "prod", Ref: "main",
		Pointer: catalog.TablePointer{
			ContentID: "empty", Namespace: "db", Name: "empty",
			MetadataLocation: "s3://wh/db/empty/metadata/v1.metadata.json",
			Metadata:         emptyMetadataJSON(1),
		},
	}}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := c.WaitEntries(context.Background()); err != nil {
		t.Fatalf("wait: %v", err)
	}
	prefix := "catalog=prod,namespace=db,ref=main,table=empty"
	got := samples(t, c)
	requireGauge(t, got, "iceberg_table_has_snapshot{"+prefix+"}", 0)
	requireGauge(t, got, "iceberg_table_partitions{"+prefix+"}", 0)
	requireGauge(t, got, "iceberg_table_small_files_ratio{"+prefix+"}", 0)
	requireGauge(t, got, "iceberg_table_partition_skew_ratio{"+prefix+"}", 0)
	requireGauge(t, got, "iceberg_table_delete_file_bytes{catalog=prod,content=position,namespace=db,ref=main,table=empty}", 0)
	requireGauge(t, got, "iceberg_exporter_manifest_queue{}", 0)
}

func TestEmbeddedManifestEntriesUseNilReaderAsEntryError(t *testing.T) {
	t.Parallel()
	c := syncCollector(t, nil, EntriesConfig{})
	if err := c.Update(context.Background(), []Table{{
		Catalog: "prod", Ref: "main",
		Pointer: catalog.TablePointer{
			ContentID: "c", Namespace: "db", Name: "events",
			MetadataLocation: "s3://wh/db/events/metadata/v1.metadata.json",
			Metadata:         embeddedManifestMetadata(),
		},
	}}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := c.WaitEntries(context.Background()); err != nil {
		t.Fatalf("wait: %v", err)
	}
	got := samples(t, c)
	requireGauge(t, got, "iceberg_table_records{catalog=prod,namespace=db,ref=main,table=events}", 3)
	requireGauge(t, got, "iceberg_table_manifests{catalog=prod,content=data,namespace=db,ref=main,table=events}", 2)
	requireGauge(t, got, "iceberg_table_scrape_error{catalog=prod,namespace=db,ref=main,stage=manifest_list,table=events}", 0)
	requireGauge(t, got, "iceberg_table_scrape_error{catalog=prod,namespace=db,ref=main,stage=manifest_entries,table=events}", 1)
}

func TestBackgroundScanYieldsToMetadata(t *testing.T) {
	releaseA := make(chan struct{})
	releaseMeta := make(chan struct{})
	started := make(chan struct{})
	metaStarted := make(chan struct{})
	second := make(chan struct{})
	var onceA, onceMeta, onceB sync.Once
	reader := &memReader{files: map[string][]byte{
		currentManifestList: manifestListBytes(t, 7, []iceberg.ManifestFile{
			manifestFile(t, entryDataPath, iceberg.ManifestContentData, 0),
			manifestFile(t, entryDataPathB, iceberg.ManifestContentData, 0),
		}),
		entryDataPath:  partitionedManifest(t),
		entryDataPathB: partitionedManifest(t),
		"s3://wh/db/other/metadata/v1.metadata.json": metadataJSON(metadataOpts{
			records: 1, operation: "append", snapshots: 1, snapshotLogs: 1,
			manifestList: "s3://wh/db/other/metadata/snap-7.avro",
		}),
		"s3://wh/db/other/metadata/snap-7.avro": manifestListBytes(t, 7, []iceberg.ManifestFile{
			manifestFile(t, "s3://wh/db/other/metadata/data.avro", iceberg.ManifestContentData, 0),
		}),
		"s3://wh/db/other/metadata/data.avro": partitionedManifest(t),
	}}
	reader.beforeRead = func(location string) {
		switch location {
		case entryDataPath:
			onceA.Do(func() { close(started) })
			<-releaseA
		case entryDataPathB:
			onceB.Do(func() { close(second) })
		case "s3://wh/db/other/metadata/v1.metadata.json":
			onceMeta.Do(func() { close(metaStarted) })
			<-releaseMeta
		}
	}
	c := NewCollector(reader, WithEntries(EntriesConfig{Concurrency: 1}))
	t.Cleanup(func() {
		closeQuiet(releaseA)
		closeQuiet(releaseMeta)
		c.Close()
	})

	updated := make(chan error, 1)
	go func() {
		updated <- c.Update(context.Background(), []Table{entryTable("main", "v1.metadata.json", 7, nil)})
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("entry scan did not start")
	}
	select {
	case err := <-updated:
		if err != nil {
			t.Fatalf("update: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("update waited for the entry scan")
	}

	otherDone := make(chan error, 1)
	go func() {
		otherDone <- c.Update(context.Background(), []Table{
			entryTable("main", "v1.metadata.json", 7, nil),
			{
				Catalog: "prod", Ref: "main",
				Pointer: catalog.TablePointer{
					ContentID: "other", Namespace: "db", Name: "other",
					MetadataLocation: "s3://wh/db/other/metadata/v1.metadata.json",
					SnapshotID:       snap(7),
				},
			},
		})
	}()
	select {
	case <-metaStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("metadata read did not start")
	}
	close(releaseA)
	select {
	case <-second:
		t.Fatal("entry scan read the next manifest while metadata was loading")
	case <-time.After(150 * time.Millisecond):
	}
	close(releaseMeta)
	select {
	case err := <-otherDone:
		if err != nil {
			t.Fatalf("other update: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("metadata update did not finish")
	}
	select {
	case <-second:
	case <-time.After(2 * time.Second):
		t.Fatal("entry scan did not resume")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.WaitEntries(ctx); err != nil {
		t.Fatalf("wait: %v", err)
	}
	requireGauge(t, samples(t, c), "iceberg_table_records{catalog=prod,namespace=db,ref=main,table=other}", 1)
	requireGauge(t, samples(t, c), "iceberg_table_partitions{catalog=prod,namespace=db,ref=main,table=events}", 2)
}

func TestBackgroundQueueWhileScanIsBlocked(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	var once sync.Once
	listA := "s3://wh/a/metadata/snap-7.avro"
	listB := "s3://wh/b/metadata/snap-7.avro"
	dataA := "s3://wh/a/metadata/data.avro"
	dataB := "s3://wh/b/metadata/data.avro"
	reader := &memReader{files: map[string][]byte{
		listA: manifestListBytes(t, 7, []iceberg.ManifestFile{manifestFile(t, dataA, iceberg.ManifestContentData, 0)}),
		listB: manifestListBytes(t, 7, []iceberg.ManifestFile{manifestFile(t, dataB, iceberg.ManifestContentData, 0)}),
		dataA: partitionedManifest(t),
		dataB: partitionedManifest(t),
	}}
	reader.beforeRead = func(location string) {
		if location == dataA || location == dataB {
			once.Do(func() { close(started) })
			<-release
		}
	}
	c := NewCollector(reader, WithEntries(EntriesConfig{Concurrency: 1}))
	t.Cleanup(func() {
		closeQuiet(release)
		c.Close()
	})
	if err := c.Update(context.Background(), []Table{
		entryTableAt("db", "one", "a", listA, 7),
		entryTableAt("db", "two", "b", listB, 7),
	}); err != nil {
		t.Fatalf("update: %v", err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("scan did not start")
	}
	requireGauge(t, samples(t, c), "iceberg_exporter_manifest_queue{}", 1)
	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.WaitEntries(ctx); err != nil {
		t.Fatalf("wait: %v", err)
	}
	requireGauge(t, samples(t, c), "iceberg_exporter_manifest_queue{}", 0)
}

func syncCollector(t *testing.T, reader Reader, cfg EntriesConfig) *Collector {
	t.Helper()
	cfg.Sync = true
	c := NewCollector(reader, WithEntries(cfg))
	t.Cleanup(c.Close)
	return c
}

func entryTable(ref, file string, snapshot int64, manifestList *string) Table {
	opts := metadataOpts{
		records: 10, dataFiles: 3, fileBytes: 1400, operation: "append",
		snapshots: 1, snapshotLogs: 1, snapshotID: int(snapshot),
	}
	if manifestList != nil {
		opts.manifestList = *manifestList
	}
	return Table{
		Catalog: "prod", Ref: ref,
		Pointer: catalog.TablePointer{
			ContentID: "nessie-1", Namespace: "db", Name: "events",
			MetadataLocation: "s3://wh/db/events/metadata/" + file,
			SnapshotID:       snap(snapshot),
			Metadata:         metadataJSON(opts),
		},
	}
}

func entryTableAt(namespace, name, id string, manifestList string, snapshot int64) Table {
	opts := metadataOpts{
		records: 4, operation: "append", snapshots: 1, snapshotLogs: 1,
		snapshotID: int(snapshot), manifestList: manifestList,
	}
	return Table{
		Catalog: "prod", Ref: "main",
		Pointer: catalog.TablePointer{
			ContentID: id, Namespace: namespace, Name: name,
			MetadataLocation: "s3://wh/" + id + "/metadata/v1.metadata.json",
			SnapshotID:       snap(snapshot),
			Metadata:         metadataJSON(opts),
		},
	}
}

func partitionedManifest(t *testing.T) []byte {
	t.Helper()
	spec := idPartitionSpec()
	entries := []iceberg.ManifestEntry{
		dataEntry(t, iceberg.EntryStatusADDED, spec, "s3://wh/data/a.parquet", 100, 1, 1),
		dataEntry(t, iceberg.EntryStatusEXISTING, spec, "s3://wh/data/b.parquet", 300, 2, 1),
		dataEntry(t, iceberg.EntryStatusADDED, spec, "s3://wh/data/c.parquet", 1000, 4, 2),
		dataEntry(t, iceberg.EntryStatusDELETED, spec, "s3://wh/data/d.parquet", 5000, 9, 3),
	}
	return writeDataManifest(t, spec, entries)
}

func deleteManifest(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	spec := iceberg.NewPartitionSpecID(0)
	w, err := iceberg.NewManifestWriter(2, &buf, spec, manifestSchema(), 7,
		iceberg.WithManifestWriterContent(iceberg.ManifestContentDeletes))
	if err != nil {
		t.Fatalf("delete writer: %v", err)
	}
	for _, entry := range []iceberg.ManifestEntry{
		deleteEntry(t, iceberg.EntryContentPosDeletes, 10),
		deleteEntry(t, iceberg.EntryContentEqDeletes, 25),
		iceberg.NewManifestEntry(iceberg.EntryStatusDELETED, snapshotPtr(), seqPtr(), seqPtr(),
			deleteEntry(t, iceberg.EntryContentEqDeletes, 100).DataFile()),
	} {
		var addErr error
		switch entry.Status() {
		case iceberg.EntryStatusDELETED:
			addErr = w.Delete(entry)
		default:
			addErr = w.Add(entry)
		}
		if addErr != nil {
			t.Fatalf("add delete: %v", addErr)
		}
	}
	if _, err := w.ToManifestFile(entryDeletePath, int64(buf.Len())); err != nil {
		t.Fatalf("close deletes: %v", err)
	}
	return buf.Bytes()
}

func writeDataManifest(t *testing.T, spec iceberg.PartitionSpec, entries []iceberg.ManifestEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	if _, err := iceberg.WriteManifest("manifest.avro", &buf, 2, spec, manifestSchema(), 7, entries); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	return buf.Bytes()
}

func manifestSchema() *iceberg.Schema {
	return iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "id", Required: true, Type: iceberg.Int64Type{},
	})
}

func manifestFileSized(t *testing.T, path string, content iceberg.ManifestContent, spec int32, length int64) iceberg.ManifestFile {
	t.Helper()
	return iceberg.NewManifestFile(2, path, length, spec, 7).
		Content(content).
		SequenceNum(1, 1).
		AddedFiles(1).
		Build()
}

type histSample struct {
	count   uint64
	sum     float64
	labels  []string
	buckets map[float64]uint64
}

func histograms(t *testing.T, c prometheus.Collector) map[string]histSample {
	t.Helper()
	out := make(map[string]histSample)
	for _, family := range gather(t, c) {
		for _, metric := range family.GetMetric() {
			if metric.GetHistogram() == nil {
				continue
			}
			names := make([]string, 0, len(metric.GetLabel()))
			for _, label := range metric.GetLabel() {
				names = append(names, label.GetName())
			}
			buckets := make(map[float64]uint64, len(metric.GetHistogram().GetBucket()))
			for _, bucket := range metric.GetHistogram().GetBucket() {
				buckets[bucket.GetUpperBound()] = bucket.GetCumulativeCount()
			}
			out[family.GetName()+"{"+labelKey(metric.GetLabel())+"}"] = histSample{
				count:   metric.GetHistogram().GetSampleCount(),
				sum:     metric.GetHistogram().GetSampleSum(),
				labels:  names,
				buckets: buckets,
			}
		}
	}
	return out
}

func helpText(t *testing.T, c prometheus.Collector, name string) string {
	t.Helper()
	for _, family := range gather(t, c) {
		if family.GetName() == name {
			return family.GetHelp()
		}
	}
	t.Fatalf("missing family %s", name)
	return ""
}

func gather(t *testing.T, c prometheus.Collector) []*dto.MetricFamily {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	if err := reg.Register(c); err != nil {
		t.Fatalf("register: %v", err)
	}
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	return families
}

func closeQuiet(ch chan struct{}) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}
