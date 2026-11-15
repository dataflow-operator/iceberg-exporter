package metadata

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"iceberg-exporter/internal/catalog"
)

const currentManifestList = "s3://wh/db/events/metadata/snap-7.avro"

const tableUUID = "11111111-1111-4111-8111-111111111111"

func TestParseSnapshotSummary(t *testing.T) {
	t.Parallel()
	got, err := Parse(metadataJSON(metadataOpts{
		records: 10, dataFiles: 2, fileBytes: 1000,
		deleteFiles: 1, positionDeletes: 3, equalityDeletes: 4,
		addedFiles: 2, addedRecords: 10, addedBytes: 1000,
		snapshots: 2, metadataLogs: 2, snapshotLogs: 2,
		operation: "append",
	}))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got.ContentID != tableUUID {
		t.Fatalf("content id %q", got.ContentID)
	}
	s := got.Stats
	if !s.HasSnapshot || s.FormatVersion != 2 || s.Operation != "append" || s.SnapshotID != 7 {
		t.Fatalf("identity %+v", s)
	}
	if s.Records != 10 || s.DataFiles != 2 || s.FileSizeBytes != 1000 {
		t.Fatalf("totals %+v", s)
	}
	if s.DeleteFiles != 1 || s.PositionDeletes != 3 || s.EqualityDeletes != 4 {
		t.Fatalf("deletes %+v", s)
	}
	if s.AddedDataFiles != 2 || s.AddedRecords != 10 || s.AddedFileSizeBytes != 1000 {
		t.Fatalf("added %+v", s)
	}
	if s.Snapshots != 2 || s.MetadataLogEntries != 2 || s.SnapshotLogEntries != 2 {
		t.Fatalf("logs %+v", s)
	}
	if s.LastUpdated.UnixMilli() != 1_700_000_000_000 || s.SnapshotTime.UnixMilli() != 1_700_000_000_000 {
		t.Fatalf("times last=%s snap=%s", s.LastUpdated, s.SnapshotTime)
	}
	if got.manifestList != currentManifestList {
		t.Fatalf("manifest list %q", got.manifestList)
	}
}

func TestParseEmptyTable(t *testing.T) {
	t.Parallel()
	got, err := Parse(emptyMetadataJSON(1))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	s := got.Stats
	if s.HasSnapshot || s.Operation != "" || s.Records != 0 || s.DataFiles != 0 || s.FileSizeBytes != 0 {
		t.Fatalf("empty table looks populated: %+v", s)
	}
	if s.FormatVersion != 2 || s.MetadataLogEntries != 1 || s.Snapshots != 0 {
		t.Fatalf("logs %+v", s)
	}
}

func TestParseRejectsBadSummary(t *testing.T) {
	t.Parallel()
	raw := metadataJSON(metadataOpts{records: 1, operation: "append"})
	raw = []byte(strings.Replace(string(raw), `"total-records":"1"`, `"total-records":"nope"`, 1))
	if _, err := Parse(raw); err == nil {
		t.Fatal("non-numeric summary was accepted")
	}
	raw = metadataJSON(metadataOpts{records: 1, operation: "compact"})
	if _, err := Parse(raw); err == nil {
		t.Fatal("unknown operation was accepted")
	}
}

func TestUpdateUsesEmbeddedMetadata(t *testing.T) {
	t.Parallel()
	reader := &memReader{files: map[string][]byte{
		currentManifestList: oneDataManifest(t),
	}}
	c := NewCollector(reader)
	c.now = func() time.Time { return time.Unix(1_700_000_100, 0).UTC() }
	body := metadataJSON(metadataOpts{
		records: 10, dataFiles: 2, fileBytes: 1000,
		deleteFiles: 1, positionDeletes: 3, equalityDeletes: 4,
		addedFiles: 2, addedRecords: 10, addedBytes: 800,
		operation: "overwrite", metadataLogs: 1, snapshotLogs: 1, snapshots: 1,
	})
	loc := "s3://wh/db/events/metadata/v1.metadata.json"
	err := c.Update(context.Background(), []Table{{
		Catalog: "prod",
		Ref:     "main",
		Pointer: catalog.TablePointer{
			ContentID:        "nessie-1",
			Namespace:        "db",
			Name:             "events",
			MetadataLocation: loc,
			SnapshotID:       snap(7),
			Metadata:         body,
		},
	}})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if got := reader.Reads(); got != 1 {
		t.Fatalf("embedded metadata read the warehouse %d times, want the manifest list once", got)
	}
	if got := reader.ReadsAt(loc); got != 0 {
		t.Fatalf("metadata location was read %d times", got)
	}
	got := samples(t, c)
	prefix := `catalog=prod,namespace=db,ref=main,table=events`
	requireGauge(t, got, "iceberg_table_records{"+prefix+"}", 10)
	requireGauge(t, got, "iceberg_table_data_files{"+prefix+"}", 2)
	requireGauge(t, got, "iceberg_table_files_size_bytes{"+prefix+"}", 1000)
	requireGauge(t, got, "iceberg_table_delete_files{"+prefix+"}", 1)
	requireGauge(t, got, "iceberg_table_position_deletes{"+prefix+"}", 3)
	requireGauge(t, got, "iceberg_table_equality_deletes{"+prefix+"}", 4)
	requireGauge(t, got, "iceberg_table_last_snapshot_added_data_files{"+prefix+"}", 2)
	requireGauge(t, got, "iceberg_table_last_snapshot_added_records{"+prefix+"}", 10)
	requireGauge(t, got, "iceberg_table_last_snapshot_added_files_size_bytes{"+prefix+"}", 800)
	requireGauge(t, got, "iceberg_table_has_snapshot{"+prefix+"}", 1)
	requireGauge(t, got, "iceberg_table_format_version{"+prefix+"}", 2)
	requireGauge(t, got, "iceberg_table_snapshot_age_seconds{"+prefix+"}", 100)
	requireGauge(t, got, "iceberg_table_last_updated_timestamp_seconds{"+prefix+"}", 1_700_000_000)
	requireGauge(t, got, "iceberg_table_metadata_log_entries{"+prefix+"}", 1)
	requireGauge(t, got, "iceberg_table_last_snapshot_info{catalog=prod,namespace=db,operation=overwrite,ref=main,table=events}", 1)
	requireGauge(t, got, "iceberg_table_scrape_error{catalog=prod,namespace=db,ref=main,stage=metadata,table=events}", 0)
	requireGauge(t, got, "iceberg_table_scrape_error{catalog=prod,namespace=db,ref=main,stage=manifest_list,table=events}", 0)
	requireGauge(t, got, "iceberg_table_manifests{catalog=prod,content=data,namespace=db,ref=main,table=events}", 1)
	requireGauge(t, got, "iceberg_table_manifests{catalog=prod,content=deletes,namespace=db,ref=main,table=events}", 0)
	requireGauge(t, got, "iceberg_table_partition_specs{"+prefix+"}", 1)
	if _, ok := got["iceberg_table_last_snapshot_info{catalog=prod,namespace=db,operation=append,ref=main,table=events}"]; ok {
		t.Fatal("operation label from a different snapshot is present")
	}

	err = c.Update(context.Background(), []Table{{
		Catalog: "prod",
		Ref:     "main",
		Pointer: catalog.TablePointer{
			ContentID:        "nessie-1",
			Namespace:        "db",
			Name:             "events",
			MetadataLocation: loc,
			SnapshotID:       snap(7),
		},
	}})
	if err != nil {
		t.Fatalf("cached update: %v", err)
	}
	if got := reader.Reads(); got != 1 {
		t.Fatalf("cached snapshot read the warehouse %d times, want 1", got)
	}
	requireGauge(t, samples(t, c), "iceberg_table_records{"+prefix+"}", 10)
}

func TestUpdateSharesSnapshotAcrossRefs(t *testing.T) {
	t.Parallel()
	loc := "s3://wh/db/events/metadata/v1.metadata.json"
	body := metadataJSON(metadataOpts{records: 4, operation: "append", snapshots: 1, snapshotLogs: 1})
	reader := &memReader{files: map[string][]byte{
		loc:                 body,
		currentManifestList: oneDataManifest(t),
	}}
	c := NewCollector(reader)
	tables := []Table{
		{Catalog: "prod", Ref: "main", Pointer: catalog.TablePointer{
			ContentID: "nessie-1", Namespace: "db", Name: "events",
			MetadataLocation: loc, SnapshotID: snap(7),
		}},
		{Catalog: "prod", Ref: "dev", Pointer: catalog.TablePointer{
			ContentID: "nessie-1", Namespace: "db", Name: "events",
			MetadataLocation: loc, SnapshotID: snap(7),
		}},
	}
	if err := c.Update(context.Background(), tables); err != nil {
		t.Fatalf("update: %v", err)
	}
	if got := reader.Reads(); got != 2 {
		t.Fatalf("reads = %d, want one metadata file and one manifest list", got)
	}
	if got := reader.ReadsAt(currentManifestList); got != 1 {
		t.Fatalf("manifest list reads = %d, want 1", got)
	}
	got := samples(t, c)
	requireGauge(t, got, "iceberg_table_records{catalog=prod,namespace=db,ref=dev,table=events}", 4)
	requireGauge(t, got, "iceberg_table_records{catalog=prod,namespace=db,ref=main,table=events}", 4)
}

func TestUpdateRefetchesWhenMetadataLocationChanges(t *testing.T) {
	t.Parallel()
	first := "s3://wh/db/events/metadata/v1.metadata.json"
	second := "s3://wh/db/events/metadata/v2.metadata.json"
	reader := &memReader{files: map[string][]byte{
		first:               metadataJSON(metadataOpts{records: 4, operation: "append", metadataLogs: 1, snapshots: 1, snapshotLogs: 1}),
		second:              metadataJSON(metadataOpts{records: 9, operation: "delete", metadataLogs: 3, snapshots: 1, snapshotLogs: 1}),
		currentManifestList: oneDataManifest(t),
	}}
	c := NewCollector(reader)
	pointer := catalog.TablePointer{
		ContentID: "nessie-1", Namespace: "db", Name: "events",
		MetadataLocation: first, SnapshotID: snap(7),
	}
	if err := c.Update(context.Background(), []Table{{Catalog: "prod", Ref: "main", Pointer: pointer}}); err != nil {
		t.Fatalf("first: %v", err)
	}
	pointer.MetadataLocation = second
	if err := c.Update(context.Background(), []Table{{Catalog: "prod", Ref: "main", Pointer: pointer}}); err != nil {
		t.Fatalf("second: %v", err)
	}
	if got := reader.Reads(); got != 3 {
		t.Fatalf("reads = %d, want 3", got)
	}
	if got := reader.ReadsAt(currentManifestList); got != 1 {
		t.Fatalf("manifest list was read again for the same snapshot: %d", got)
	}
	got := samples(t, c)
	prefix := "catalog=prod,namespace=db,ref=main,table=events"
	requireGauge(t, got, "iceberg_table_records{"+prefix+"}", 9)
	requireGauge(t, got, "iceberg_table_metadata_log_entries{"+prefix+"}", 3)
	requireGauge(t, got, "iceberg_table_last_snapshot_info{catalog=prod,namespace=db,operation=delete,ref=main,table=events}", 1)
	if _, ok := got["iceberg_table_last_snapshot_info{catalog=prod,namespace=db,operation=append,ref=main,table=events}"]; ok {
		t.Fatal("previous operation is still published")
	}
	pointer.MetadataLocation = first
	if err := c.Update(context.Background(), []Table{{Catalog: "prod", Ref: "main", Pointer: pointer}}); err != nil {
		t.Fatalf("third: %v", err)
	}
	if got := reader.Reads(); got != 4 {
		t.Fatalf("reads = %d, want 4 after the unreferenced location was dropped", got)
	}
	if got := reader.ReadsAt(currentManifestList); got != 1 {
		t.Fatalf("manifest list reads = %d, want 1 after the metadata file was dropped", got)
	}
}

func TestUpdateEmptyTableIsNotAnError(t *testing.T) {
	t.Parallel()
	body := emptyMetadataJSON(2)
	c := NewCollector(&memReader{failAll: true})
	err := c.Update(context.Background(), []Table{{
		Catalog: "prod",
		Ref:     "main",
		Pointer: catalog.TablePointer{
			ContentID: "nessie-1", Namespace: "db", Name: "empty",
			Metadata: body,
		},
	}})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	got := samples(t, c)
	prefix := "catalog=prod,namespace=db,ref=main,table=empty"
	requireGauge(t, got, "iceberg_table_has_snapshot{"+prefix+"}", 0)
	requireGauge(t, got, "iceberg_table_records{"+prefix+"}", 0)
	requireGauge(t, got, "iceberg_table_data_files{"+prefix+"}", 0)
	requireGauge(t, got, "iceberg_table_snapshot_age_seconds{"+prefix+"}", 0)
	requireGauge(t, got, "iceberg_table_metadata_log_entries{"+prefix+"}", 2)
	requireGauge(t, got, "iceberg_table_scrape_error{catalog=prod,namespace=db,ref=main,stage=metadata,table=empty}", 0)
	requireGauge(t, got, "iceberg_table_scrape_error{catalog=prod,namespace=db,ref=main,stage=manifest_list,table=empty}", 0)
	requireGauge(t, got, "iceberg_table_manifests{catalog=prod,content=data,namespace=db,ref=main,table=empty}", 0)
	requireGauge(t, got, "iceberg_table_manifests{catalog=prod,content=deletes,namespace=db,ref=main,table=empty}", 0)
	requireGauge(t, got, "iceberg_table_partition_specs{"+prefix+"}", 0)
	for name := range got {
		if strings.Contains(name, "last_snapshot_info") {
			t.Fatalf("empty table published %s", name)
		}
	}
}

func TestUpdateIsolatesTableErrors(t *testing.T) {
	t.Parallel()
	goodLoc := "s3://wh/db/good/metadata/v1.metadata.json"
	badLoc := "s3://wh/db/bad/metadata/v1.metadata.json"
	reader := &memReader{
		files: map[string][]byte{
			goodLoc:             metadataJSON(metadataOpts{records: 5, operation: "append", snapshots: 1, snapshotLogs: 1}),
			currentManifestList: oneDataManifest(t),
		},
		errAt: map[string]error{badLoc: errors.New("warehouse down")},
	}
	c := NewCollector(reader)
	err := c.Update(context.Background(), []Table{
		{Catalog: "prod", Ref: "main", Pointer: catalog.TablePointer{
			ContentID: "good", Namespace: "db", Name: "good", MetadataLocation: goodLoc, SnapshotID: snap(7),
		}},
		{Catalog: "prod", Ref: "main", Pointer: catalog.TablePointer{
			ContentID: "bad", Namespace: "db", Name: "bad", MetadataLocation: badLoc, SnapshotID: snap(7),
		}},
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	got := samples(t, c)
	requireGauge(t, got, "iceberg_table_records{catalog=prod,namespace=db,ref=main,table=good}", 5)
	requireGauge(t, got, "iceberg_table_scrape_error{catalog=prod,namespace=db,ref=main,stage=metadata,table=good}", 0)
	requireGauge(t, got, "iceberg_table_scrape_error{catalog=prod,namespace=db,ref=main,stage=manifest_list,table=good}", 0)
	requireGauge(t, got, "iceberg_table_scrape_error{catalog=prod,namespace=db,ref=main,stage=metadata,table=bad}", 1)
	if _, ok := got["iceberg_table_scrape_error{catalog=prod,namespace=db,ref=main,stage=manifest_list,table=bad}"]; ok {
		t.Fatal("metadata failure also published a manifest list stage")
	}
	if _, ok := got["iceberg_table_records{catalog=prod,namespace=db,ref=main,table=bad}"]; ok {
		t.Fatal("failed table published zero records")
	}

	badBody := []byte("{")
	err = c.Update(context.Background(), []Table{{
		Catalog: "prod",
		Ref:     "main",
		Pointer: catalog.TablePointer{
			ContentID: "bad", Namespace: "db", Name: "bad", Metadata: badBody,
		},
	}})
	if err != nil {
		t.Fatalf("bad json: %v", err)
	}
	if got := reader.Reads(); got != 3 {
		t.Fatalf("invalid embedded json was read from the warehouse %d times", got)
	}
}

func TestUpdateKeepsLastSummaryWhenRefreshFails(t *testing.T) {
	t.Parallel()
	first := "s3://wh/db/events/metadata/v1.metadata.json"
	second := "s3://wh/db/events/metadata/v2.metadata.json"
	reader := &memReader{files: map[string][]byte{
		first:               metadataJSON(metadataOpts{records: 5, operation: "append", snapshots: 1, snapshotLogs: 1}),
		currentManifestList: oneDataManifest(t),
	}}
	c := NewCollector(reader)
	pointer := catalog.TablePointer{
		ContentID: "nessie-1", Namespace: "db", Name: "events",
		MetadataLocation: first, SnapshotID: snap(7),
	}
	if err := c.Update(context.Background(), []Table{{Catalog: "prod", Ref: "main", Pointer: pointer}}); err != nil {
		t.Fatalf("first: %v", err)
	}
	reader.errAt = map[string]error{second: errors.New("timeout")}
	pointer.MetadataLocation = second
	if err := c.Update(context.Background(), []Table{{Catalog: "prod", Ref: "main", Pointer: pointer}}); err != nil {
		t.Fatalf("second: %v", err)
	}
	got := samples(t, c)
	prefix := "catalog=prod,namespace=db,ref=main,table=events"
	requireGauge(t, got, "iceberg_table_records{"+prefix+"}", 5)
	requireGauge(t, got, "iceberg_table_scrape_error{catalog=prod,namespace=db,ref=main,stage=metadata,table=events}", 1)
	requireGauge(t, got, "iceberg_table_scrape_error{catalog=prod,namespace=db,ref=main,stage=manifest_list,table=events}", 0)
	requireGauge(t, got, "iceberg_table_manifests{catalog=prod,content=data,namespace=db,ref=main,table=events}", 1)
}

func TestUpdateDropsRemovedTablesAndViews(t *testing.T) {
	t.Parallel()
	body := metadataJSON(metadataOpts{records: 1, operation: "replace", snapshots: 1, snapshotLogs: 1})
	c := NewCollector(&memReader{files: map[string][]byte{currentManifestList: oneDataManifest(t)}})
	keep := Table{Catalog: "prod", Ref: "main", Pointer: catalog.TablePointer{
		ContentID: "keep", Namespace: "db", Name: "keep", Metadata: body,
	}}
	drop := Table{Catalog: "prod", Ref: "main", Pointer: catalog.TablePointer{
		ContentID: "drop", Namespace: "db", Name: "drop", Metadata: body,
	}}
	view := Table{Catalog: "prod", Ref: "main", Pointer: catalog.TablePointer{
		ContentID: "view", Namespace: "db", Name: "view", Type: catalog.ContentView, Metadata: body,
	}}
	if err := c.Update(context.Background(), []Table{keep, drop, view}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := c.Update(context.Background(), []Table{keep}); err != nil {
		t.Fatalf("retain: %v", err)
	}
	got := samples(t, c)
	if _, ok := got["iceberg_table_records{catalog=prod,namespace=db,ref=main,table=keep}"]; !ok {
		t.Fatal("kept table disappeared")
	}
	if _, ok := got["iceberg_table_records{catalog=prod,namespace=db,ref=main,table=drop}"]; ok {
		t.Fatal("removed table is still published")
	}
	if _, ok := got["iceberg_table_records{catalog=prod,namespace=db,ref=main,table=view}"]; ok {
		t.Fatal("view was published")
	}
}

func TestUpdateCancelKeepsPublishedSet(t *testing.T) {
	t.Parallel()
	body := metadataJSON(metadataOpts{records: 6, operation: "append", snapshots: 1, snapshotLogs: 1})
	c := NewCollector(&memReader{files: map[string][]byte{currentManifestList: oneDataManifest(t)}})
	if err := c.Update(context.Background(), []Table{{
		Catalog: "prod", Ref: "main",
		Pointer: catalog.TablePointer{ContentID: "c", Namespace: "db", Name: "events", Metadata: body},
	}}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := c.Update(ctx, []Table{{
		Catalog: "prod", Ref: "main",
		Pointer: catalog.TablePointer{
			ContentID: "c", Namespace: "db", Name: "events",
			MetadataLocation: "s3://wh/db/events/metadata/v2.metadata.json",
		},
	}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	requireGauge(t, samples(t, c), "iceberg_table_records{catalog=prod,namespace=db,ref=main,table=events}", 6)
	requireGauge(t, samples(t, c), "iceberg_table_scrape_error{catalog=prod,namespace=db,ref=main,stage=metadata,table=events}", 0)
	requireGauge(t, samples(t, c), "iceberg_table_scrape_error{catalog=prod,namespace=db,ref=main,stage=manifest_list,table=events}", 0)
}

type memReader struct {
	mu         sync.Mutex
	reads      int
	byLocation map[string]int
	files      map[string][]byte
	errAt      map[string]error
	failAll    bool
	beforeRead func(location string)
}

func (m *memReader) Read(ctx context.Context, location string) ([]byte, error) {
	if m.beforeRead != nil {
		m.beforeRead(location)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reads++
	if m.byLocation == nil {
		m.byLocation = make(map[string]int)
	}
	m.byLocation[location]++
	if m.failAll {
		return nil, errors.New("reader disabled")
	}
	if err, ok := m.errAt[location]; ok {
		return nil, err
	}
	body, ok := m.files[location]
	if !ok {
		return nil, fmt.Errorf("missing metadata")
	}
	return body, nil
}

func (m *memReader) Reads() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.reads
}

func (m *memReader) ReadsAt(location string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.byLocation[location]
}

func samples(t *testing.T, c prometheus.Collector) map[string]float64 {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	if err := reg.Register(c); err != nil {
		t.Fatalf("register: %v", err)
	}
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	out := make(map[string]float64)
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			out[family.GetName()+"{"+labelKey(metric.GetLabel())+"}"] = metric.GetGauge().GetValue()
		}
	}
	return out
}

func labelKey(labels []*dto.LabelPair) string {
	parts := make([]string, 0, len(labels))
	for _, label := range labels {
		parts = append(parts, label.GetName()+"="+label.GetValue())
	}
	// Gather sorts labels by name.
	return strings.Join(parts, ",")
}

func requireGauge(t *testing.T, got map[string]float64, name string, want float64) {
	t.Helper()
	value, ok := got[name]
	if !ok {
		t.Fatalf("missing %s in %v", name, got)
	}
	if value != want {
		t.Fatalf("%s = %v, want %v", name, value, want)
	}
}

func snap(id int64) *int64 { return &id }

func TestParseManifestListCountsContentAndSpecs(t *testing.T) {
	t.Parallel()
	raw := manifestListBytes(t, 7, []iceberg.ManifestFile{
		manifestFile(t, "s3://wh/data-0.avro", iceberg.ManifestContentData, 0),
		manifestFile(t, "s3://wh/data-1.avro", iceberg.ManifestContentData, 3),
		manifestFile(t, "s3://wh/deletes.avro", iceberg.ManifestContentDeletes, 3),
	})
	got, err := ParseManifestList(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got.Data != 2 || got.Deletes != 1 || got.PartitionSpecs != 2 {
		t.Fatalf("counts %+v", got)
	}
}

func TestParseManifestListV1IsData(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	file := iceberg.NewManifestFile(1, "s3://wh/a.avro", 10, 2, 7).AddedFiles(1).Build()
	if err := iceberg.WriteManifestList(1, &buf, 7, nil, nil, 0, []iceberg.ManifestFile{file}); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := ParseManifestList(buf.Bytes())
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got.Data != 1 || got.Deletes != 0 || got.PartitionSpecs != 1 {
		t.Fatalf("counts %+v", got)
	}
}

func TestUpdateNilReaderFailsManifestListOnly(t *testing.T) {
	t.Parallel()
	c := NewCollector(nil)
	err := c.Update(context.Background(), []Table{{
		Catalog: "prod", Ref: "main",
		Pointer: catalog.TablePointer{
			ContentID: "nessie-1", Namespace: "db", Name: "events",
			Metadata: metadataJSON(metadataOpts{records: 4, operation: "append", snapshots: 1, snapshotLogs: 1}),
		},
	}})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	got := samples(t, c)
	prefix := "catalog=prod,namespace=db,ref=main,table=events"
	requireGauge(t, got, "iceberg_table_records{"+prefix+"}", 4)
	requireGauge(t, got, "iceberg_table_scrape_error{catalog=prod,namespace=db,ref=main,stage=metadata,table=events}", 0)
	requireGauge(t, got, "iceberg_table_scrape_error{catalog=prod,namespace=db,ref=main,stage=manifest_list,table=events}", 1)
	if _, ok := got["iceberg_table_manifests{catalog=prod,content=data,namespace=db,ref=main,table=events}"]; ok {
		t.Fatal("nil reader published manifest counts")
	}
}

func TestParseManifestListEmptyIsZero(t *testing.T) {
	t.Parallel()
	raw := manifestListBytes(t, 7, nil)
	got, err := ParseManifestList(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got != (ManifestCounts{}) {
		t.Fatalf("counts %+v", got)
	}
	if _, err := ParseManifestList(nil); err == nil {
		t.Fatal("empty file was accepted")
	}
	if _, err := ParseManifestList([]byte("not avro")); err == nil {
		t.Fatal("garbage was accepted")
	}
}

func TestUpdatePublishesManifestCounts(t *testing.T) {
	t.Parallel()
	list := manifestListBytes(t, 7, []iceberg.ManifestFile{
		manifestFile(t, "s3://wh/data-0.avro", iceberg.ManifestContentData, 0),
		manifestFile(t, "s3://wh/data-1.avro", iceberg.ManifestContentData, 4),
		manifestFile(t, "s3://wh/deletes.avro", iceberg.ManifestContentDeletes, 4),
	})
	body := metadataJSON(metadataOpts{records: 10, operation: "append", snapshots: 1, snapshotLogs: 1})
	reader := &memReader{files: map[string][]byte{currentManifestList: list}}
	c := NewCollector(reader)
	err := c.Update(context.Background(), []Table{{
		Catalog: "prod", Ref: "main",
		Pointer: catalog.TablePointer{
			ContentID: "nessie-1", Namespace: "db", Name: "events",
			MetadataLocation: "s3://wh/db/events/metadata/v1.metadata.json",
			SnapshotID:       snap(7),
			Metadata:         body,
		},
	}})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	got := samples(t, c)
	prefix := "catalog=prod,namespace=db,ref=main,table=events"
	requireGauge(t, got, "iceberg_table_manifests{catalog=prod,content=data,namespace=db,ref=main,table=events}", 2)
	requireGauge(t, got, "iceberg_table_manifests{catalog=prod,content=deletes,namespace=db,ref=main,table=events}", 1)
	requireGauge(t, got, "iceberg_table_partition_specs{"+prefix+"}", 2)
	requireGauge(t, got, "iceberg_table_records{"+prefix+"}", 10)
	requireGauge(t, got, "iceberg_table_scrape_error{catalog=prod,namespace=db,ref=main,stage=manifest_list,table=events}", 0)
}

func TestUpdateManifestListFailureKeepsMetadata(t *testing.T) {
	t.Parallel()
	loc := "s3://wh/db/events/metadata/v1.metadata.json"
	body := metadataJSON(metadataOpts{records: 10, operation: "append", snapshots: 1, snapshotLogs: 1})
	reader := &memReader{files: map[string][]byte{loc: body}}
	c := NewCollector(reader)
	pointer := catalog.TablePointer{
		ContentID: "nessie-1", Namespace: "db", Name: "events",
		MetadataLocation: loc, SnapshotID: snap(7),
	}
	if err := c.Update(context.Background(), []Table{{Catalog: "prod", Ref: "main", Pointer: pointer}}); err != nil {
		t.Fatalf("update: %v", err)
	}
	got := samples(t, c)
	prefix := "catalog=prod,namespace=db,ref=main,table=events"
	requireGauge(t, got, "iceberg_table_records{"+prefix+"}", 10)
	requireGauge(t, got, "iceberg_table_scrape_error{catalog=prod,namespace=db,ref=main,stage=metadata,table=events}", 0)
	requireGauge(t, got, "iceberg_table_scrape_error{catalog=prod,namespace=db,ref=main,stage=manifest_list,table=events}", 1)
	if _, ok := got["iceberg_table_manifests{catalog=prod,content=data,namespace=db,ref=main,table=events}"]; ok {
		t.Fatal("failed manifest list still published a count")
	}
	if _, ok := got["iceberg_table_partition_specs{"+prefix+"}"]; ok {
		t.Fatal("failed manifest list still published partition specs")
	}

	reader.files[currentManifestList] = oneDataManifest(t)
	if err := c.Update(context.Background(), []Table{{Catalog: "prod", Ref: "main", Pointer: pointer}}); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if got := reader.ReadsAt(loc); got != 1 {
		t.Fatalf("metadata was read again after a manifest failure: %d", got)
	}
	got = samples(t, c)
	requireGauge(t, got, "iceberg_table_manifests{catalog=prod,content=data,namespace=db,ref=main,table=events}", 1)
	requireGauge(t, got, "iceberg_table_scrape_error{catalog=prod,namespace=db,ref=main,stage=manifest_list,table=events}", 0)
}

func TestUpdateNewSnapshotReadsAnotherManifestList(t *testing.T) {
	t.Parallel()
	first := "s3://wh/db/events/metadata/v1.metadata.json"
	second := "s3://wh/db/events/metadata/v2.metadata.json"
	nextList := "s3://wh/db/events/metadata/snap-8.avro"
	reader := &memReader{files: map[string][]byte{
		first: metadataJSON(metadataOpts{records: 4, operation: "append", snapshots: 1, snapshotLogs: 1}),
		second: metadataJSON(metadataOpts{
			records: 9, operation: "append", snapshots: 1, snapshotLogs: 1,
			snapshotID: 8, manifestList: nextList,
		}),
		currentManifestList: oneDataManifest(t),
		nextList: manifestListBytes(t, 8, []iceberg.ManifestFile{
			manifestFile(t, "s3://wh/data-a.avro", iceberg.ManifestContentData, 0),
			manifestFile(t, "s3://wh/deletes-a.avro", iceberg.ManifestContentDeletes, 0),
		}),
	}}
	c := NewCollector(reader)
	pointer := catalog.TablePointer{
		ContentID: "nessie-1", Namespace: "db", Name: "events",
		MetadataLocation: first, SnapshotID: snap(7),
	}
	if err := c.Update(context.Background(), []Table{{Catalog: "prod", Ref: "main", Pointer: pointer}}); err != nil {
		t.Fatalf("first: %v", err)
	}
	pointer.MetadataLocation = second
	pointer.SnapshotID = snap(8)
	if err := c.Update(context.Background(), []Table{{Catalog: "prod", Ref: "main", Pointer: pointer}}); err != nil {
		t.Fatalf("second: %v", err)
	}
	if got := reader.ReadsAt(currentManifestList); got != 1 {
		t.Fatalf("old manifest list reads = %d", got)
	}
	if got := reader.ReadsAt(nextList); got != 1 {
		t.Fatalf("new manifest list reads = %d", got)
	}
	got := samples(t, c)
	prefix := "catalog=prod,namespace=db,ref=main,table=events"
	requireGauge(t, got, "iceberg_table_records{"+prefix+"}", 9)
	requireGauge(t, got, "iceberg_table_manifests{catalog=prod,content=data,namespace=db,ref=main,table=events}", 1)
	requireGauge(t, got, "iceberg_table_manifests{catalog=prod,content=deletes,namespace=db,ref=main,table=events}", 1)
}

func TestUpdateDroppingTableDropsManifestCache(t *testing.T) {
	t.Parallel()
	body := metadataJSON(metadataOpts{records: 2, operation: "append", snapshots: 1, snapshotLogs: 1})
	reader := &memReader{files: map[string][]byte{currentManifestList: oneDataManifest(t)}}
	c := NewCollector(reader)
	table := Table{Catalog: "prod", Ref: "main", Pointer: catalog.TablePointer{
		ContentID: "nessie-1", Namespace: "db", Name: "events",
		MetadataLocation: "s3://wh/db/events/metadata/v1.metadata.json",
		SnapshotID:       snap(7), Metadata: body,
	}}
	if err := c.Update(context.Background(), []Table{table}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := c.Update(context.Background(), nil); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if err := c.Update(context.Background(), []Table{table}); err != nil {
		t.Fatalf("return: %v", err)
	}
	if got := reader.ReadsAt(currentManifestList); got != 2 {
		t.Fatalf("manifest list reads = %d, want 2 after the table left the catalog", got)
	}
}

func TestUpdateEmbeddedManifestPaths(t *testing.T) {
	t.Parallel()
	c := NewCollector(&memReader{failAll: true})
	err := c.Update(context.Background(), []Table{{
		Catalog: "prod", Ref: "main",
		Pointer: catalog.TablePointer{
			ContentID: "v1", Namespace: "db", Name: "events",
			Metadata: embeddedManifestMetadata(),
		},
	}})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	got := samples(t, c)
	requireGauge(t, got, "iceberg_table_manifests{catalog=prod,content=data,namespace=db,ref=main,table=events}", 2)
	requireGauge(t, got, "iceberg_table_manifests{catalog=prod,content=deletes,namespace=db,ref=main,table=events}", 0)
	requireGauge(t, got, "iceberg_table_partition_specs{catalog=prod,namespace=db,ref=main,table=events}", 2)
	requireGauge(t, got, "iceberg_table_scrape_error{catalog=prod,namespace=db,ref=main,stage=manifest_list,table=events}", 0)
	requireGauge(t, got, "iceberg_table_records{catalog=prod,namespace=db,ref=main,table=events}", 3)
}

func TestUpdateCancelDuringManifestReadKeepsPublishedSet(t *testing.T) {
	t.Parallel()
	body := metadataJSON(metadataOpts{records: 6, operation: "append", snapshots: 1, snapshotLogs: 1})
	next := metadataJSON(metadataOpts{
		records: 9, operation: "append", snapshots: 1, snapshotLogs: 1,
		snapshotID: 8, manifestList: "s3://wh/db/events/metadata/snap-8.avro",
	})
	reader := &memReader{files: map[string][]byte{currentManifestList: oneDataManifest(t)}}
	c := NewCollector(reader)
	if err := c.Update(context.Background(), []Table{{
		Catalog: "prod", Ref: "main",
		Pointer: catalog.TablePointer{
			ContentID: "c", Namespace: "db", Name: "events",
			MetadataLocation: "s3://wh/db/events/metadata/v1.metadata.json",
			SnapshotID:       snap(7), Metadata: body,
		},
	}}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	nextList := "s3://wh/db/events/metadata/snap-8.avro"
	reader.beforeRead = func(location string) {
		if location == nextList {
			cancel()
		}
	}
	err := c.Update(ctx, []Table{{
		Catalog: "prod", Ref: "main",
		Pointer: catalog.TablePointer{
			ContentID: "c", Namespace: "db", Name: "events",
			MetadataLocation: "s3://wh/db/events/metadata/v2.metadata.json",
			SnapshotID:       snap(8), Metadata: next,
		},
	}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	requireGauge(t, samples(t, c), "iceberg_table_records{catalog=prod,namespace=db,ref=main,table=events}", 6)
	requireGauge(t, samples(t, c), "iceberg_table_manifests{catalog=prod,content=data,namespace=db,ref=main,table=events}", 1)
	if got := reader.ReadsAt(nextList); got != 0 {
		t.Fatalf("cancelled update read the next manifest list %d times", got)
	}
}

func manifestListBytes(t *testing.T, snapshotID int64, files []iceberg.ManifestFile) []byte {
	t.Helper()
	seq := snapshotID
	var buf bytes.Buffer
	if err := iceberg.WriteManifestList(2, &buf, snapshotID, nil, &seq, 0, files); err != nil {
		t.Fatalf("write manifest list: %v", err)
	}
	return buf.Bytes()
}

func oneDataManifest(t *testing.T) []byte {
	t.Helper()
	return manifestListBytes(t, 7, []iceberg.ManifestFile{
		manifestFile(t, "s3://wh/db/events/metadata/data-0.avro", iceberg.ManifestContentData, 0),
	})
}

func manifestFile(t *testing.T, path string, content iceberg.ManifestContent, spec int32) iceberg.ManifestFile {
	t.Helper()
	return iceberg.NewManifestFile(2, path, 16, spec, 7).
		Content(content).
		SequenceNum(1, 1).
		AddedFiles(1).
		Build()
}

func embeddedManifestMetadata() []byte {
	return []byte(`{
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
		"partition-specs": [
			{"spec-id": 0, "fields": []},
			{"spec-id": 1, "fields": [{"name": "id", "transform": "identity", "source-id": 1, "field-id": 1000}]}
		],
		"default-spec-id": 1,
		"last-partition-id": 1000,
		"properties": {},
		"sort-orders": [{"order-id": 0, "fields": []}],
		"default-sort-order-id": 0,
		"current-snapshot-id": 7,
		"snapshots": [{
			"snapshot-id": 7,
			"sequence-number": 1,
			"timestamp-ms": 1700000000000,
			"manifests": [
				"s3://wh/db/events/metadata/a.avro",
				"s3://wh/db/events/metadata/b.avro"
			],
			"summary": {
				"operation": "append",
				"total-records": "3",
				"added-data-files": "2",
				"added-records": "3",
				"added-files-size": "10"
			}
		}],
		"snapshot-log": [{"snapshot-id": 7, "timestamp-ms": 1700000000000}]
	}`)
}

type metadataOpts struct {
	records         int
	dataFiles       int
	fileBytes       int
	deleteFiles     int
	positionDeletes int
	equalityDeletes int
	addedFiles      int
	addedRecords    int
	addedBytes      int
	snapshots       int
	metadataLogs    int
	snapshotLogs    int
	operation       string
	snapshotID      int
	manifestList    string
}

func metadataJSON(opts metadataOpts) []byte {
	if opts.operation == "" {
		opts.operation = "append"
	}
	if opts.snapshots < 1 {
		opts.snapshots = 1
	}
	if opts.snapshotLogs < 1 {
		opts.snapshotLogs = 1
	}
	currentID := 7
	if opts.snapshotID != 0 {
		currentID = opts.snapshotID
	}
	var snaps strings.Builder
	for i := 1; i <= opts.snapshots; i++ {
		if i > 1 {
			snaps.WriteString(",")
		}
		id := i
		manifest := fmt.Sprintf("s3://wh/db/events/metadata/snap-%d.avro", id)
		summary := `"operation":"append","added-data-files":"0","added-records":"0","added-files-size":"0"`
		if i == opts.snapshots {
			id = currentID
			manifest = fmt.Sprintf("s3://wh/db/events/metadata/snap-%d.avro", id)
			if opts.manifestList != "" {
				manifest = opts.manifestList
			}
			summary = fmt.Sprintf(
				`"operation":%q,"total-records":%q,"total-data-files":%q,"total-files-size":%q,"total-delete-files":%q,"total-position-deletes":%q,"total-equality-deletes":%q,"added-data-files":%q,"added-records":%q,"added-files-size":%q`,
				opts.operation,
				strconv.Itoa(opts.records),
				strconv.Itoa(opts.dataFiles),
				strconv.Itoa(opts.fileBytes),
				strconv.Itoa(opts.deleteFiles),
				strconv.Itoa(opts.positionDeletes),
				strconv.Itoa(opts.equalityDeletes),
				strconv.Itoa(opts.addedFiles),
				strconv.Itoa(opts.addedRecords),
				strconv.Itoa(opts.addedBytes),
			)
		}
		fmt.Fprintf(&snaps, `{
			"snapshot-id": %d,
			"sequence-number": %d,
			"timestamp-ms": 1700000000000,
			"manifest-list": %q,
			"summary": {%s}
		}`, id, i, manifest, summary)
	}
	logs := func(n int, kind string) string {
		var b strings.Builder
		for i := 1; i <= n; i++ {
			if i > 1 {
				b.WriteString(",")
			}
			if kind == "metadata" {
				fmt.Fprintf(&b, `{"metadata-file":"s3://wh/db/events/metadata/v%d.metadata.json","timestamp-ms":1690000000000}`, i)
				continue
			}
			fmt.Fprintf(&b, `{"snapshot-id":%d,"timestamp-ms":1700000000000}`, i)
		}
		return b.String()
	}
	raw := fmt.Sprintf(`{
		"format-version": 2,
		"table-uuid": %q,
		"location": "s3://wh/db/events",
		"last-sequence-number": %d,
		"last-updated-ms": 1700000000000,
		"last-column-id": 1,
		"schemas": [{
			"schema-id": 0,
			"type": "struct",
			"fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]
		}],
		"current-schema-id": 0,
		"partition-specs": [{"spec-id": 0, "fields": []}],
		"default-spec-id": 0,
		"last-partition-id": 999,
		"properties": {},
		"sort-orders": [{"order-id": 0, "fields": []}],
		"default-sort-order-id": 0,
		"current-snapshot-id": %d,
		"refs": {"main": {"snapshot-id": %d, "type": "branch"}},
		"snapshots": [%s],
		"snapshot-log": [%s],
		"metadata-log": [%s]
	}`, tableUUID, opts.snapshots, currentID, currentID, snaps.String(), logs(opts.snapshotLogs, "snapshot"), logs(opts.metadataLogs, "metadata"))
	return []byte(raw)
}

func emptyMetadataJSON(metadataLogs int) []byte {
	var logs strings.Builder
	for i := 1; i <= metadataLogs; i++ {
		if i > 1 {
			logs.WriteString(",")
		}
		fmt.Fprintf(&logs, `{"metadata-file":"s3://wh/db/empty/metadata/v%d.metadata.json","timestamp-ms":1690000000000}`, i)
	}
	raw := fmt.Sprintf(`{
		"format-version": 2,
		"table-uuid": %q,
		"location": "s3://wh/db/empty",
		"last-sequence-number": 0,
		"last-updated-ms": 1700000000000,
		"last-column-id": 1,
		"schemas": [{
			"schema-id": 0,
			"type": "struct",
			"fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]
		}],
		"current-schema-id": 0,
		"partition-specs": [{"spec-id": 0, "fields": []}],
		"default-spec-id": 0,
		"last-partition-id": 999,
		"properties": {},
		"sort-orders": [{"order-id": 0, "fields": []}],
		"default-sort-order-id": 0,
		"snapshots": [],
		"snapshot-log": [],
		"metadata-log": [%s]
	}`, tableUUID, logs.String())
	return []byte(raw)
}
