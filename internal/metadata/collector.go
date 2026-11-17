package metadata

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/prometheus/client_golang/prometheus"

	"iceberg-exporter/internal/catalog"
)

const (
	// StageMetadata is the scrape_error stage for a metadata JSON failure.
	StageMetadata = "metadata"
	// StageManifestList is the scrape_error stage for a manifest list failure.
	// Metadata gauges are still published when only this stage fails.
	StageManifestList = "manifest_list"
)

const (
	manifestContentData    = "data"
	manifestContentDeletes = "deletes"
)

// Reader loads metadata JSON from a warehouse location.
// The location may carry credentials in its query string. Implementations
// must not log it.
type Reader interface {
	Read(ctx context.Context, location string) ([]byte, error)
}

// Table is one catalog pointer to publish.
// Catalog and Ref are metric labels. The pointer supplies namespace, name,
// content id, snapshot id, metadataLocation, and metadata JSON when the
// adapter already loaded it.
type Table struct {
	Catalog string
	Ref     string
	Pointer catalog.TablePointer
}

// Collector caches metadata summaries and exports them as gauges.
// Update replaces the whole published set. Collect does not read the warehouse.
type Collector struct {
	reader Reader
	now    func() time.Time

	mu         sync.RWMutex
	series     map[seriesKey]published
	snaps      map[snapKey]map[string]tableMeta
	byLocation map[string]snapKey
	manifests  map[snapKey]manifestSnapshot

	entries       entrySettings
	entryQueue    []entryJob
	entryQueued   map[snapKey]struct{}
	entryInflight map[snapKey]struct{}
	entryState    map[snapKey]entryState
	entryCond     *sync.Cond
	workerCtx     context.Context
	workerCancel  context.CancelFunc
	workerWG      sync.WaitGroup
	closeOnce     sync.Once
	gateMu        sync.Mutex
	gateCond      *sync.Cond
	updating      bool

	descs metricDescs
}

type snapKey struct {
	contentID   string
	snapshotID  int64
	hasSnapshot bool
}

type seriesKey struct {
	catalog   string
	ref       string
	namespace string
	table     string
}

type published struct {
	stats          Stats
	ok             bool
	failed         bool
	manifestsReady bool
	manifestFailed bool
	key            snapKey
	location       string
	keyed          bool
}

// tableMeta is the cached metadata JSON summary.
// Manifest counts are not stored here: they are keyed only by snapshot, so a
// metadata rewrite that keeps the snapshot does not read the list again.
type tableMeta struct {
	stats             Stats
	manifestList      string
	embeddedPresent   bool
	embeddedManifests int
	embeddedPaths     []string
	metadataSpecs     int
}

// NewCollector returns an empty collector. reader may be nil when every
// pointer already carries metadata JSON and no entry scan is configured.
// WithEntries starts the background manifest-entry scan.
func NewCollector(reader Reader, opts ...Option) *Collector {
	c := &Collector{
		reader:        reader,
		now:           time.Now,
		series:        make(map[seriesKey]published),
		snaps:         make(map[snapKey]map[string]tableMeta),
		byLocation:    make(map[string]snapKey),
		manifests:     make(map[snapKey]manifestSnapshot),
		entryQueued:   make(map[snapKey]struct{}),
		entryInflight: make(map[snapKey]struct{}),
		entryState:    make(map[snapKey]entryState),
	}
	c.entryCond = sync.NewCond(&c.mu)
	c.gateCond = sync.NewCond(&c.gateMu)
	for _, opt := range opts {
		opt(c)
	}
	small := c.entries.small
	if small <= 0 {
		small = DefaultSmallFileBytes
	}
	c.descs = newDescs(small)
	c.startEntryWorkers()
	return c
}

// Update loads metadata for tables and replaces the published set.
// A table whose pointer already contains metadata JSON is not read from the
// warehouse. A cache hit on content id, snapshot id, and metadataLocation is
// not read either. The manifest list is cached by content id and snapshot id.
// One table's read or parse error is recorded on that table
// and does not fail the others. The previous summary is kept when a later
// metadata read fails. A manifest list failure keeps the new metadata gauges
// and sets the manifest_list stage.
//
// A cancelled context leaves the previously published set in place.
// Views are omitted. Pass every table that should remain on the next scrape;
// a table left out of tables is removed.
func (c *Collector) Update(ctx context.Context, tables []Table) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Metadata and the manifest list run here. Entry workers wait for new
	// reads until this returns, and this does not wait for them.
	c.pauseEntries()
	defer c.resumeEntries()
	c.mu.RLock()
	prev := make(map[seriesKey]published, len(c.series))
	for k, v := range c.series {
		prev[k] = v
	}
	c.mu.RUnlock()

	next := make(map[seriesKey]published, len(tables))
	for _, table := range tables {
		if err := ctx.Err(); err != nil {
			return err
		}
		if table.Pointer.Type == catalog.ContentView || table.Pointer.Name == "" {
			continue
		}
		key := seriesKey{
			catalog:   table.Catalog,
			ref:       table.Ref,
			namespace: table.Pointer.Namespace,
			table:     table.Pointer.Name,
		}
		pub, err := c.load(ctx, table)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			kept := published{failed: true}
			if old, ok := prev[key]; ok && old.ok {
				kept = old
				kept.failed = true
			}
			next[key] = kept
			continue
		}
		next[key] = pub
	}

	c.mu.Lock()
	c.series = next
	c.sweepLocked()
	c.enqueueEntriesLocked(c.now())
	c.mu.Unlock()
	return nil
}

func (c *Collector) load(ctx context.Context, table Table) (published, error) {
	p := table.Pointer
	meta, key, ok := c.cached(p)
	if !ok {
		raw := []byte(p.Metadata)
		if len(raw) == 0 {
			if p.MetadataLocation == "" {
				return published{}, errors.New("metadata: empty metadata location")
			}
			if c.reader == nil {
				return published{}, errors.New("metadata: nil reader")
			}
			body, err := c.reader.Read(ctx, p.MetadataLocation)
			if err != nil {
				return published{}, err
			}
			raw = body
		}
		parsedDoc, err := Parse(raw)
		if err != nil {
			return published{}, err
		}
		key = snapKey{
			contentID:   identity(p, parsedDoc.ContentID),
			snapshotID:  parsedDoc.Stats.SnapshotID,
			hasSnapshot: parsedDoc.Stats.HasSnapshot,
		}
		meta = tableMeta{
			stats:             parsedDoc.Stats,
			manifestList:      parsedDoc.manifestList,
			embeddedPresent:   parsedDoc.embeddedPresent,
			embeddedManifests: parsedDoc.embeddedManifests,
			embeddedPaths:     parsedDoc.embeddedPaths,
			metadataSpecs:     parsedDoc.metadataSpecs,
		}
		c.store(key, p.MetadataLocation, meta)
	}
	pub := published{
		stats:    meta.stats,
		ok:       true,
		key:      key,
		location: p.MetadataLocation,
		keyed:    true,
	}
	if !meta.stats.HasSnapshot {
		pub.manifestsReady = true
		return pub, nil
	}
	snap, err := c.resolveManifest(ctx, key, meta)
	if err != nil {
		if ctx.Err() != nil {
			return published{}, ctx.Err()
		}
		pub.manifestFailed = true
		return pub, nil
	}
	pub.stats.DataManifests = snap.counts.Data
	pub.stats.DeleteManifests = snap.counts.Deletes
	pub.stats.PartitionSpecs = snap.counts.PartitionSpecs
	pub.manifestsReady = true
	return pub, nil
}

func identity(p catalog.TablePointer, tableUUID string) string {
	if p.ContentID != "" {
		return p.ContentID
	}
	if tableUUID != "" {
		return tableUUID
	}
	return p.Namespace + "\x00" + p.Name
}

func (c *Collector) cached(p catalog.TablePointer) (tableMeta, snapKey, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if p.MetadataLocation != "" {
		if key, ok := c.byLocation[p.MetadataLocation]; ok {
			if meta, ok := c.snaps[key][p.MetadataLocation]; ok {
				return meta, key, true
			}
		}
	}
	id := p.ContentID
	if id == "" {
		return tableMeta{}, snapKey{}, false
	}
	key := snapKey{contentID: id, hasSnapshot: p.SnapshotID != nil}
	if p.SnapshotID != nil {
		key.snapshotID = *p.SnapshotID
	}
	meta, ok := c.snaps[key][p.MetadataLocation]
	if !ok {
		return tableMeta{}, snapKey{}, false
	}
	return meta, key, true
}

func (c *Collector) store(key snapKey, location string, meta tableMeta) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if location != "" {
		if prev, ok := c.byLocation[location]; ok && prev != key {
			delete(c.snaps[prev], location)
			if len(c.snaps[prev]) == 0 {
				delete(c.snaps, prev)
			}
		}
		c.byLocation[location] = key
	}
	locs := c.snaps[key]
	if locs == nil {
		locs = make(map[string]tableMeta)
		c.snaps[key] = locs
	}
	locs[location] = meta
}

func (c *Collector) resolveManifest(ctx context.Context, key snapKey, meta tableMeta) (manifestSnapshot, error) {
	c.mu.RLock()
	snap, ok := c.manifests[key]
	c.mu.RUnlock()
	if ok {
		return snap, nil
	}
	snap, err := c.readManifest(ctx, meta)
	if err != nil {
		return manifestSnapshot{}, err
	}
	c.mu.Lock()
	c.manifests[key] = snap
	c.mu.Unlock()
	return snap, nil
}

func (c *Collector) readManifest(ctx context.Context, meta tableMeta) (manifestSnapshot, error) {
	if meta.manifestList == "" {
		if meta.embeddedPresent {
			files := make([]manifestObject, len(meta.embeddedPaths))
			for i, path := range meta.embeddedPaths {
				files[i] = manifestObject{
					path:     path,
					content:  iceberg.ManifestContentData,
					version:  1,
					embedded: true,
				}
			}
			return manifestSnapshot{
				counts: embeddedManifestCounts(meta.embeddedManifests, meta.metadataSpecs),
				files:  files,
			}, nil
		}
		return manifestSnapshot{}, errors.New("manifest list: snapshot has no manifest list")
	}
	if c.reader == nil {
		return manifestSnapshot{}, errors.New("manifest list: nil reader")
	}
	body, err := c.reader.Read(ctx, meta.manifestList)
	if err != nil {
		return manifestSnapshot{}, err
	}
	return parseManifestSnapshot(body)
}

func (c *Collector) sweepLocked() {
	live := make(map[snapKey]map[string]struct{})
	for _, pub := range c.series {
		if !pub.keyed || !pub.ok {
			continue
		}
		locs := live[pub.key]
		if locs == nil {
			locs = make(map[string]struct{})
			live[pub.key] = locs
		}
		locs[pub.location] = struct{}{}
	}
	for key, locs := range c.snaps {
		keep := live[key]
		for location := range locs {
			if _, ok := keep[location]; ok {
				continue
			}
			delete(locs, location)
			if c.byLocation[location] == key {
				delete(c.byLocation, location)
			}
		}
		if len(locs) == 0 {
			delete(c.snaps, key)
		}
	}
	liveManifests := make(map[snapKey]struct{})
	for _, pub := range c.series {
		if pub.manifestsReady && pub.stats.HasSnapshot {
			liveManifests[pub.key] = struct{}{}
		}
	}
	for key := range c.manifests {
		if _, ok := liveManifests[key]; !ok {
			delete(c.manifests, key)
		}
	}
	c.sweepEntriesLocked()
}

// Describe implements prometheus.Collector.
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	c.descs.describe(ch)
}

// QueueStale enqueues manifest-entry scans whose cache is missing or older
// than the refresh interval. It does not read the warehouse. A scrape calls
// it so file-level work is scheduled while Collect still serves memory.
func (c *Collector) QueueStale(now time.Time) {
	if c == nil || !c.entries.enabled {
		return
	}
	if now.IsZero() {
		now = c.now()
	}
	c.mu.Lock()
	c.enqueueEntriesLocked(now)
	c.mu.Unlock()
}

// Collect implements prometheus.Collector.
// Gauges come from memory. Snapshot age is measured at collect time.
// Collect does not read the warehouse and does not enqueue scans.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	c.mu.RLock()
	now := c.now()
	queue := len(c.entryQueue)
	items := make([]struct {
		key     seriesKey
		pub     published
		entries entryState
		scanned bool
	}, 0, len(c.series))
	for key, pub := range c.series {
		item := struct {
			key     seriesKey
			pub     published
			entries entryState
			scanned bool
		}{key: key, pub: pub}
		if c.entries.enabled && pub.keyed {
			if st, ok := c.entryState[pub.key]; ok {
				item.entries = st
				item.scanned = true
			}
		}
		items = append(items, item)
	}
	c.mu.RUnlock()
	ch <- prometheus.MustNewConstMetric(c.descs.manifestQueue, prometheus.GaugeValue, float64(queue))
	for _, item := range items {
		c.emit(ch, item.key, item.pub, item.entries, item.scanned, now)
	}
}

func (c *Collector) emit(ch chan<- prometheus.Metric, key seriesKey, pub published, entries entryState, scanned bool, now time.Time) {
	labels := []string{key.catalog, key.ref, key.namespace, key.table}
	errValue := 0.0
	if pub.failed {
		errValue = 1
	}
	ch <- prometheus.MustNewConstMetric(c.descs.scrapeError, prometheus.GaugeValue, errValue, append(append([]string{}, labels...), StageMetadata)...)
	if pub.ok || pub.manifestFailed {
		manifestErr := 0.0
		if pub.manifestFailed {
			manifestErr = 1
		}
		ch <- prometheus.MustNewConstMetric(c.descs.scrapeError, prometheus.GaugeValue, manifestErr, append(append([]string{}, labels...), StageManifestList)...)
	}
	if !pub.ok {
		return
	}
	stats := pub.stats
	has := 0.0
	if stats.HasSnapshot {
		has = 1
	}
	ch <- prometheus.MustNewConstMetric(c.descs.hasSnapshot, prometheus.GaugeValue, has, labels...)
	ch <- prometheus.MustNewConstMetric(c.descs.formatVersion, prometheus.GaugeValue, float64(stats.FormatVersion), labels...)
	c.emitWriteConfig(ch, labels, stats)
	ch <- prometheus.MustNewConstMetric(c.descs.snapshotAge, prometheus.GaugeValue, ageSeconds(stats, now), labels...)
	ch <- prometheus.MustNewConstMetric(c.descs.lastUpdated, prometheus.GaugeValue, unixSeconds(stats.LastUpdated), labels...)
	ch <- prometheus.MustNewConstMetric(c.descs.snapshots, prometheus.GaugeValue, float64(stats.Snapshots), labels...)
	ch <- prometheus.MustNewConstMetric(c.descs.metadataLog, prometheus.GaugeValue, float64(stats.MetadataLogEntries), labels...)
	ch <- prometheus.MustNewConstMetric(c.descs.snapshotLog, prometheus.GaugeValue, float64(stats.SnapshotLogEntries), labels...)
	ch <- prometheus.MustNewConstMetric(c.descs.records, prometheus.GaugeValue, float64(stats.Records), labels...)
	ch <- prometheus.MustNewConstMetric(c.descs.dataFiles, prometheus.GaugeValue, float64(stats.DataFiles), labels...)
	ch <- prometheus.MustNewConstMetric(c.descs.fileSize, prometheus.GaugeValue, float64(stats.FileSizeBytes), labels...)
	ch <- prometheus.MustNewConstMetric(c.descs.deleteFiles, prometheus.GaugeValue, float64(stats.DeleteFiles), labels...)
	ch <- prometheus.MustNewConstMetric(c.descs.positionDeletes, prometheus.GaugeValue, float64(stats.PositionDeletes), labels...)
	ch <- prometheus.MustNewConstMetric(c.descs.equalityDeletes, prometheus.GaugeValue, float64(stats.EqualityDeletes), labels...)
	ch <- prometheus.MustNewConstMetric(c.descs.addedDataFiles, prometheus.GaugeValue, float64(stats.AddedDataFiles), labels...)
	ch <- prometheus.MustNewConstMetric(c.descs.addedRecords, prometheus.GaugeValue, float64(stats.AddedRecords), labels...)
	ch <- prometheus.MustNewConstMetric(c.descs.addedFileSize, prometheus.GaugeValue, float64(stats.AddedFileSizeBytes), labels...)
	if stats.Operation != "" {
		ch <- prometheus.MustNewConstMetric(c.descs.operation, prometheus.GaugeValue, 1, append(append([]string{}, labels...), stats.Operation)...)
	}
	if !pub.manifestsReady {
		return
	}
	ch <- prometheus.MustNewConstMetric(c.descs.partitionSpecs, prometheus.GaugeValue, float64(stats.PartitionSpecs), labels...)
	ch <- prometheus.MustNewConstMetric(c.descs.manifests, prometheus.GaugeValue, float64(stats.DataManifests), append(append([]string{}, labels...), manifestContentData)...)
	ch <- prometheus.MustNewConstMetric(c.descs.manifests, prometheus.GaugeValue, float64(stats.DeleteManifests), append(append([]string{}, labels...), manifestContentDeletes)...)
	if scanned {
		c.emitEntries(ch, labels, entries, now)
	}
}

func (c *Collector) emitWriteConfig(ch chan<- prometheus.Metric, labels []string, stats Stats) {
	ch <- prometheus.MustNewConstMetric(
		c.descs.writeConfig, prometheus.GaugeValue, 1,
		append(append([]string{}, labels...),
			stats.Format,
			stats.DeleteMode,
			stats.UpdateMode,
			stats.MergeMode,
			stats.DistributionMode,
			stats.Compression,
		)...,
	)
	if stats.TargetFileSizeSet {
		ch <- prometheus.MustNewConstMetric(c.descs.targetFileSize, prometheus.GaugeValue, float64(stats.TargetFileSize), labels...)
	}
	if stats.RowGroupSizeSet {
		ch <- prometheus.MustNewConstMetric(c.descs.rowGroupSize, prometheus.GaugeValue, float64(stats.RowGroupSize), labels...)
	}
	if stats.PageSizeSet {
		ch <- prometheus.MustNewConstMetric(c.descs.pageSize, prometheus.GaugeValue, float64(stats.PageSize), labels...)
	}
	if stats.ManifestTargetSet {
		ch <- prometheus.MustNewConstMetric(c.descs.manifestTarget, prometheus.GaugeValue, float64(stats.ManifestTarget), labels...)
	}
	ch <- prometheus.MustNewConstMetric(c.descs.partitionFields, prometheus.GaugeValue, float64(stats.PartitionFields), labels...)
	ch <- prometheus.MustNewConstMetric(
		c.descs.partitionSpec, prometheus.GaugeValue, 1,
		append(append([]string{}, labels...), strconv.Itoa(stats.PartitionSpecID), stats.PartitionTransforms)...,
	)
}

func ageSeconds(stats Stats, now time.Time) float64 {
	if !stats.HasSnapshot || stats.SnapshotTime.IsZero() {
		return 0
	}
	age := now.Sub(stats.SnapshotTime).Seconds()
	if age < 0 {
		return 0
	}
	return age
}

func unixSeconds(t time.Time) float64 {
	if t.IsZero() {
		return 0
	}
	return float64(t.Unix())
}

type metricDescs struct {
	hasSnapshot      *prometheus.Desc
	formatVersion    *prometheus.Desc
	snapshotAge      *prometheus.Desc
	lastUpdated      *prometheus.Desc
	snapshots        *prometheus.Desc
	metadataLog      *prometheus.Desc
	snapshotLog      *prometheus.Desc
	records          *prometheus.Desc
	dataFiles        *prometheus.Desc
	fileSize         *prometheus.Desc
	deleteFiles      *prometheus.Desc
	positionDeletes  *prometheus.Desc
	equalityDeletes  *prometheus.Desc
	addedDataFiles   *prometheus.Desc
	addedRecords     *prometheus.Desc
	addedFileSize    *prometheus.Desc
	operation        *prometheus.Desc
	partitionSpecs   *prometheus.Desc
	manifests        *prometheus.Desc
	scrapeError      *prometheus.Desc
	fileSizeHist     *prometheus.Desc
	smallRatio       *prometheus.Desc
	partitions       *prometheus.Desc
	partitionMax     *prometheus.Desc
	partitionSkew    *prometheus.Desc
	deleteBytes      *prometheus.Desc
	manifestCacheAge *prometheus.Desc
	manifestQueue    *prometheus.Desc
	writeConfig      *prometheus.Desc
	targetFileSize   *prometheus.Desc
	rowGroupSize     *prometheus.Desc
	pageSize         *prometheus.Desc
	manifestTarget   *prometheus.Desc
	partitionFields  *prometheus.Desc
	partitionSpec    *prometheus.Desc
}

func newDescs(smallFileBytes int64) metricDescs {
	labels := []string{"catalog", "ref", "namespace", "table"}
	return metricDescs{
		hasSnapshot: prometheus.NewDesc(
			"iceberg_table_has_snapshot",
			"1 when the table has a current snapshot.",
			labels, nil,
		),
		formatVersion: prometheus.NewDesc(
			"iceberg_table_format_version",
			"Iceberg table format version from metadata JSON.",
			labels, nil,
		),
		snapshotAge: prometheus.NewDesc(
			"iceberg_table_snapshot_age_seconds",
			"Age of the current snapshot. Zero when the table has no snapshot.",
			labels, nil,
		),
		lastUpdated: prometheus.NewDesc(
			"iceberg_table_last_updated_timestamp_seconds",
			"Unix time when table metadata was last updated.",
			labels, nil,
		),
		snapshots: prometheus.NewDesc(
			"iceberg_table_snapshots",
			"Number of snapshots retained in metadata.",
			labels, nil,
		),
		metadataLog: prometheus.NewDesc(
			"iceberg_table_metadata_log_entries",
			"Number of previous metadata files in the metadata log.",
			labels, nil,
		),
		snapshotLog: prometheus.NewDesc(
			"iceberg_table_snapshot_log_entries",
			"Number of entries in the snapshot log.",
			labels, nil,
		),
		records: prometheus.NewDesc(
			"iceberg_table_records",
			"Records in the current snapshot summary. Zero when the table has no snapshot.",
			labels, nil,
		),
		dataFiles: prometheus.NewDesc(
			"iceberg_table_data_files",
			"Data files in the current snapshot summary.",
			labels, nil,
		),
		fileSize: prometheus.NewDesc(
			"iceberg_table_files_size_bytes",
			"Total data file size in bytes from the current snapshot summary.",
			labels, nil,
		),
		deleteFiles: prometheus.NewDesc(
			"iceberg_table_delete_files",
			"Delete files in the current snapshot summary.",
			labels, nil,
		),
		positionDeletes: prometheus.NewDesc(
			"iceberg_table_position_deletes",
			"Position deletes in the current snapshot summary.",
			labels, nil,
		),
		equalityDeletes: prometheus.NewDesc(
			"iceberg_table_equality_deletes",
			"Equality deletes in the current snapshot summary.",
			labels, nil,
		),
		addedDataFiles: prometheus.NewDesc(
			"iceberg_table_last_snapshot_added_data_files",
			"Data files added by the current snapshot.",
			labels, nil,
		),
		addedRecords: prometheus.NewDesc(
			"iceberg_table_last_snapshot_added_records",
			"Records added by the current snapshot.",
			labels, nil,
		),
		addedFileSize: prometheus.NewDesc(
			"iceberg_table_last_snapshot_added_files_size_bytes",
			"Bytes added by the current snapshot.",
			labels, nil,
		),
		operation: prometheus.NewDesc(
			"iceberg_table_last_snapshot_info",
			"1 for the operation that produced the current snapshot.",
			append(append([]string{}, labels...), "operation"), nil,
		),
		partitionSpecs: prometheus.NewDesc(
			"iceberg_table_partition_specs",
			"Distinct partition specs referenced by the current snapshot manifest list. Paths without spec ids use the specs defined in table metadata. Zero when the table has no snapshot.",
			labels, nil,
		),
		manifests: prometheus.NewDesc(
			"iceberg_table_manifests",
			"Manifest files in the current snapshot manifest list. Cached by content id and snapshot id.",
			append(append([]string{}, labels...), "content"), nil,
		),
		scrapeError: prometheus.NewDesc(
			"iceberg_table_scrape_error",
			"1 when the latest read for the table failed at this stage. One table does not fail the scrape.",
			append(append([]string{}, labels...), "stage"), nil,
		),
		fileSizeHist: prometheus.NewDesc(
			"iceberg_table_file_size_bytes",
			"Size of live data files (status added or existing) in the current snapshot. Buckets are 8MiB, 32MiB, 128MiB, 512MiB, and 1GiB. Served from the manifest-entry cache on every scrape.",
			labels, nil,
		),
		smallRatio: prometheus.NewDesc(
			"iceberg_table_small_files_ratio",
			fmt.Sprintf("Share of live data files smaller than %d bytes. Served from the manifest-entry cache on every scrape.", smallFileBytes),
			labels, nil,
		),
		partitions: prometheus.NewDesc(
			"iceberg_table_partitions",
			"Distinct partitions among live data files. Partition values are not labels. Served from the manifest-entry cache on every scrape.",
			labels, nil,
		),
		partitionMax: prometheus.NewDesc(
			"iceberg_table_partition_bytes_max",
			"Largest partition size in bytes among live data files. Served from the manifest-entry cache on every scrape.",
			labels, nil,
		),
		partitionSkew: prometheus.NewDesc(
			"iceberg_table_partition_skew_ratio",
			"max(partition bytes) / mean(partition bytes) for live data files. Zero when the snapshot has no data bytes. Served from the manifest-entry cache on every scrape.",
			labels, nil,
		),
		deleteBytes: prometheus.NewDesc(
			"iceberg_table_delete_file_bytes",
			"Bytes in live delete files of the current snapshot. Present when delete-manifest scans are enabled. Served from the manifest-entry cache on every scrape.",
			append(append([]string{}, labels...), "content"), nil,
		),
		manifestCacheAge: prometheus.NewDesc(
			"iceberg_table_manifest_cache_age_seconds",
			"Seconds since the manifest-entry cache for this table was computed. File-level series are republished from this cache on every scrape.",
			labels, nil,
		),
		manifestQueue: prometheus.NewDesc(
			"iceberg_exporter_manifest_queue",
			"Manifest-entry scans waiting to read the warehouse. File-level table metrics are served from an in-memory cache.",
			nil, nil,
		),
		writeConfig: prometheus.NewDesc(
			"iceberg_table_write_config",
			"1 for the write properties stored in table metadata. A label is unset when the key is absent and other when the value is outside the known set. Writer session defaults are not visible.",
			append(append([]string{}, labels...), "format", "delete_mode", "update_mode", "merge_mode", "distribution_mode", "compression"), nil,
		),
		targetFileSize: prometheus.NewDesc(
			"iceberg_table_write_target_file_size_bytes",
			"write.target-file-size-bytes from table properties, when it is an integer greater than 0. Absent when the property is unset or invalid.",
			labels, nil,
		),
		rowGroupSize: prometheus.NewDesc(
			"iceberg_table_parquet_row_group_size_bytes",
			"write.parquet.row-group-size-bytes from table properties, when it is an integer greater than 0. Absent when the property is unset or invalid.",
			labels, nil,
		),
		pageSize: prometheus.NewDesc(
			"iceberg_table_parquet_page_size_bytes",
			"write.parquet.page-size-bytes from table properties, when it is an integer greater than 0. Absent when the property is unset or invalid.",
			labels, nil,
		),
		manifestTarget: prometheus.NewDesc(
			"iceberg_table_manifest_target_size_bytes",
			"commit.manifest.target-size-bytes from table properties, when it is an integer greater than 0. Absent when the property is unset or invalid.",
			labels, nil,
		),
		partitionFields: prometheus.NewDesc(
			"iceberg_table_partition_fields",
			"Number of fields in the current partition spec. Zero when the spec has no fields.",
			labels, nil,
		),
		partitionSpec: prometheus.NewDesc(
			"iceberg_table_partition_spec",
			"1 for the current partition spec. transforms lists field transforms in order, without column names. The value is none when the spec has no fields. More than 8 fields are truncated.",
			append(append([]string{}, labels...), "spec_id", "transforms"), nil,
		),
	}
}

func (d metricDescs) describe(ch chan<- *prometheus.Desc) {
	for _, desc := range []*prometheus.Desc{
		d.hasSnapshot,
		d.formatVersion,
		d.snapshotAge,
		d.lastUpdated,
		d.snapshots,
		d.metadataLog,
		d.snapshotLog,
		d.records,
		d.dataFiles,
		d.fileSize,
		d.deleteFiles,
		d.positionDeletes,
		d.equalityDeletes,
		d.addedDataFiles,
		d.addedRecords,
		d.addedFileSize,
		d.operation,
		d.partitionSpecs,
		d.manifests,
		d.scrapeError,
		d.fileSizeHist,
		d.smallRatio,
		d.partitions,
		d.partitionMax,
		d.partitionSkew,
		d.deleteBytes,
		d.manifestCacheAge,
		d.manifestQueue,
		d.writeConfig,
		d.targetFileSize,
		d.rowGroupSize,
		d.pageSize,
		d.manifestTarget,
		d.partitionFields,
		d.partitionSpec,
	} {
		ch <- desc
	}
}
