package metadata

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/avro/ocf"
)

const (
	// DefaultSmallFileBytes is the live data file size treated as small.
	// A file of exactly this size is not small. The threshold is part of the
	// metric help text, not a label.
	DefaultSmallFileBytes int64 = 32 << 20
	// DefaultRefreshInterval is how long a file-level cache is reused.
	// Snapshots are immutable; the interval bounds retries after a failed scan.
	DefaultRefreshInterval = time.Hour
	// DefaultEntryConcurrency is the warehouse read pool for manifest entries.
	// Metadata and manifest-list reads run in Update and are not queued behind it.
	DefaultEntryConcurrency = 2
	// DefaultEntryQueue is the maximum number of snapshots waiting for an entry scan.
	DefaultEntryQueue = 256
	// DefaultMaxManifestBytes is the warehouse budget for one snapshot scan.
	DefaultMaxManifestBytes int64 = 512 << 20

	// StageManifestEntries is the scrape_error stage for a manifest entry failure.
	// Metadata and manifest-list gauges are still published.
	StageManifestEntries = "manifest_entries"

	deleteContentPosition = "position"
	deleteContentEquality = "equality"
)

// fileSizeBucketBounds are the fixed histogram bounds for data-file sizes.
var fileSizeBucketBounds = []float64{
	8 << 20,
	32 << 20,
	128 << 20,
	512 << 20,
	1 << 30,
}

var errManifestBytes = errors.New("manifest entries: max manifest bytes exceeded")

// EntriesConfig controls the background manifest-entry scan.
// Zero values use the defaults above. Sync drains the queue on WaitEntries
// instead of starting workers, which keeps tests on the caller goroutine.
type EntriesConfig struct {
	RefreshInterval time.Duration
	Concurrency     int
	MaxQueue        int
	MaxBytes        int64
	SmallFileBytes  int64
	// DeleteManifests reads delete manifests and publishes
	// iceberg_table_delete_file_bytes. Data-file metrics do not use them:
	// snapshot summary already counts delete files.
	DeleteManifests bool
	Sync            bool
}

type entrySettings struct {
	enabled     bool
	refresh     time.Duration
	concurrency int
	maxQueue    int
	maxBytes    int64
	small       int64
	deletes     bool
	sync        bool
}

// WithEntries enables background aggregation of manifest entries.
func WithEntries(cfg EntriesConfig) Option {
	return func(c *Collector) {
		c.entries = normalizeEntries(cfg)
	}
}

func normalizeEntries(cfg EntriesConfig) entrySettings {
	s := entrySettings{
		enabled:     true,
		refresh:     cfg.RefreshInterval,
		concurrency: cfg.Concurrency,
		maxQueue:    cfg.MaxQueue,
		maxBytes:    cfg.MaxBytes,
		small:       cfg.SmallFileBytes,
		deletes:     cfg.DeleteManifests,
		sync:        cfg.Sync,
	}
	if s.refresh <= 0 {
		s.refresh = DefaultRefreshInterval
	}
	if s.concurrency <= 0 {
		s.concurrency = DefaultEntryConcurrency
	}
	if s.maxQueue <= 0 {
		s.maxQueue = DefaultEntryQueue
	}
	if s.maxBytes <= 0 {
		s.maxBytes = DefaultMaxManifestBytes
	}
	if s.small <= 0 {
		s.small = DefaultSmallFileBytes
	}
	return s
}

// manifestObject is one file in the current snapshot manifest list.
// embedded v1 paths have no spec id until the Avro header is read.
type manifestObject struct {
	path     string
	content  iceberg.ManifestContent
	specID   int32
	length   int64
	version  int
	embedded bool
}

type manifestSnapshot struct {
	counts ManifestCounts
	files  []manifestObject
}

type entryJob struct {
	key   snapKey
	files []manifestObject
}

type entryState struct {
	agg      fileAggregate
	ok       bool
	failed   bool
	at       time.Time
	failedAt time.Time
}

// fileAggregate is the file-level cache for one snapshot.
// Partition values are reduced to max and skew and then discarded.
type fileAggregate struct {
	count         uint64
	sum           float64
	buckets       []uint64
	files         int64
	small         int64
	partitions    int
	maxBytes      int64
	skew          float64
	positionBytes int64
	equalityBytes int64
	deletes       bool
	at            time.Time
}

type partBytes struct {
	files   int64
	bytes   int64
	records int64
}

type accumulator struct {
	parts    map[string]*partBytes
	buckets  []uint64
	count    uint64
	bytes    int64
	files    int64
	small    int64
	position int64
	equality int64
}

func (c *Collector) startEntryWorkers() {
	if !c.entries.enabled || c.entries.sync {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.workerCtx = ctx
	c.workerCancel = cancel
	for range c.entries.concurrency {
		c.workerWG.Add(1)
		go func() {
			defer c.workerWG.Done()
			c.entryWorker()
		}()
	}
}

func (c *Collector) entryWorker() {
	for {
		job, ok := c.takeJob()
		if !ok {
			return
		}
		agg, err := c.scanJob(c.workerCtx, job)
		c.finishJob(job.key, agg, err)
	}
}

func (c *Collector) takeJob() (entryJob, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for len(c.entryQueue) == 0 && c.workerCtx.Err() == nil {
		c.entryCond.Wait()
	}
	if c.workerCtx.Err() != nil {
		return entryJob{}, false
	}
	return c.popLocked(), true
}

func (c *Collector) popLocked() entryJob {
	job := c.entryQueue[0]
	c.entryQueue[0] = entryJob{}
	c.entryQueue = c.entryQueue[1:]
	delete(c.entryQueued, job.key)
	c.entryInflight[job.key] = struct{}{}
	return job
}

// WaitEntries blocks until queued entry scans have been applied.
// In Sync mode the scans run on this goroutine. A cancelled context leaves
// unfinished snapshots unmarked, so the next Update can enqueue them again.
func (c *Collector) WaitEntries(ctx context.Context) error {
	if !c.entries.enabled {
		return nil
	}
	if c.entries.sync {
		return c.drainEntries(ctx)
	}
	stop := context.AfterFunc(ctx, func() {
		c.mu.Lock()
		c.entryCond.Broadcast()
		c.mu.Unlock()
	})
	defer stop()
	c.mu.Lock()
	defer c.mu.Unlock()
	for (len(c.entryQueue) > 0 || len(c.entryInflight) > 0) && ctx.Err() == nil {
		c.entryCond.Wait()
	}
	return ctx.Err()
}

func (c *Collector) drainEntries(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		c.mu.Lock()
		if len(c.entryQueue) == 0 {
			c.mu.Unlock()
			return nil
		}
		job := c.popLocked()
		c.mu.Unlock()
		agg, err := c.scanJob(ctx, job)
		c.finishJob(job.key, agg, err)
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
	}
}

// Close stops entry workers. It is safe to call more than once.
// Sync collectors have no workers; Close still lets WaitEntries return.
func (c *Collector) Close() {
	c.closeOnce.Do(func() {
		if c.workerCancel != nil {
			c.workerCancel()
		}
		c.mu.Lock()
		if c.entryCond != nil {
			c.entryCond.Broadcast()
		}
		c.mu.Unlock()
		c.gateMu.Lock()
		if c.gateCond != nil {
			c.gateCond.Broadcast()
		}
		c.gateMu.Unlock()
		c.workerWG.Wait()
	})
}

func (c *Collector) pauseEntries() {
	if !c.entries.enabled || c.entries.sync {
		return
	}
	c.gateMu.Lock()
	c.updating = true
	c.gateMu.Unlock()
}

func (c *Collector) resumeEntries() {
	if !c.entries.enabled || c.entries.sync {
		return
	}
	c.gateMu.Lock()
	c.updating = false
	c.gateCond.Broadcast()
	c.gateMu.Unlock()
}

// waitTurn keeps a new entry read from starting while Update is loading
// metadata or a manifest list. A read already inside the warehouse finishes.
func (c *Collector) waitTurn(ctx context.Context) error {
	if !c.entries.enabled || c.entries.sync {
		return ctx.Err()
	}
	stop := context.AfterFunc(ctx, func() {
		c.gateMu.Lock()
		c.gateCond.Broadcast()
		c.gateMu.Unlock()
	})
	defer stop()
	c.gateMu.Lock()
	defer c.gateMu.Unlock()
	for c.updating && ctx.Err() == nil {
		c.gateCond.Wait()
	}
	return ctx.Err()
}

func (c *Collector) enqueueEntriesLocked(now time.Time) {
	if !c.entries.enabled {
		return
	}
	for _, pub := range c.series {
		if !pub.ok || !pub.keyed {
			continue
		}
		if !pub.stats.HasSnapshot {
			// Nothing to read. Refresh the observation so cache age stays current.
			c.entryState[pub.key] = successState(emptyAggregate(c.entries.deletes, now), now)
			continue
		}
		if !pub.manifestsReady {
			continue
		}
		files := c.filesToScan(c.manifests[pub.key].files)
		if len(files) == 0 {
			c.entryState[pub.key] = successState(emptyAggregate(c.entries.deletes, now), now)
			continue
		}
		if !c.shouldEnqueueLocked(pub.key, now) {
			continue
		}
		if len(c.entryQueue) >= c.entries.maxQueue {
			continue
		}
		c.entryQueue = append(c.entryQueue, entryJob{key: pub.key, files: files})
		c.entryQueued[pub.key] = struct{}{}
	}
	if c.entryCond != nil {
		c.entryCond.Broadcast()
	}
}

func successState(agg fileAggregate, now time.Time) entryState {
	agg.at = now
	return entryState{agg: agg, ok: true, at: now}
}

func (c *Collector) shouldEnqueueLocked(key snapKey, now time.Time) bool {
	if _, ok := c.entryQueued[key]; ok {
		return false
	}
	if _, ok := c.entryInflight[key]; ok {
		return false
	}
	st, ok := c.entryState[key]
	if !ok {
		return true
	}
	if st.failed {
		return now.Sub(st.failedAt) >= c.entries.refresh
	}
	return now.Sub(st.at) >= c.entries.refresh
}

func (c *Collector) filesToScan(files []manifestObject) []manifestObject {
	out := make([]manifestObject, 0, len(files))
	for _, file := range files {
		switch file.content {
		case iceberg.ManifestContentData:
			out = append(out, file)
		case iceberg.ManifestContentDeletes:
			if c.entries.deletes {
				out = append(out, file)
			}
		}
	}
	return out
}

func (c *Collector) finishJob(key snapKey, agg fileAggregate, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entryInflight, key)
	defer c.entryCond.Broadcast()
	if !c.snapshotLiveLocked(key) {
		delete(c.entryState, key)
		return
	}
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		st := c.entryState[key]
		st.failed = true
		st.failedAt = c.now()
		c.entryState[key] = st
		return
	}
	c.entryState[key] = successState(agg, c.now())
}

func (c *Collector) snapshotLiveLocked(key snapKey) bool {
	for _, pub := range c.series {
		if pub.keyed && pub.ok && pub.key == key {
			return true
		}
	}
	return false
}

func (c *Collector) sweepEntriesLocked() {
	live := make(map[snapKey]struct{})
	for _, pub := range c.series {
		if pub.keyed && pub.ok {
			live[pub.key] = struct{}{}
		}
	}
	for key := range c.entryState {
		if _, ok := live[key]; !ok {
			delete(c.entryState, key)
		}
	}
	kept := c.entryQueue[:0]
	for _, job := range c.entryQueue {
		if _, ok := live[job.key]; !ok {
			delete(c.entryQueued, job.key)
			continue
		}
		kept = append(kept, job)
	}
	for i := len(kept); i < len(c.entryQueue); i++ {
		c.entryQueue[i] = entryJob{}
	}
	c.entryQueue = kept
}

func (c *Collector) scanJob(ctx context.Context, job entryJob) (fileAggregate, error) {
	if c.reader == nil {
		return fileAggregate{}, errors.New("manifest entries: nil reader")
	}
	acc := accumulator{
		parts:   make(map[string]*partBytes),
		buckets: make([]uint64, len(fileSizeBucketBounds)),
	}
	var used int64
	for _, file := range job.files {
		if err := ctx.Err(); err != nil {
			return fileAggregate{}, err
		}
		if err := c.waitTurn(ctx); err != nil {
			return fileAggregate{}, err
		}
		if file.length > 0 && used+file.length > c.entries.maxBytes {
			return fileAggregate{}, errManifestBytes
		}
		body, err := c.reader.Read(ctx, file.path)
		if err != nil {
			return fileAggregate{}, fmt.Errorf("manifest entries: %w", err)
		}
		n := int64(len(body))
		if n < 0 || used > c.entries.maxBytes-n {
			return fileAggregate{}, errManifestBytes
		}
		used += n
		if err := acc.addManifest(file, body, c.entries.small); err != nil {
			return fileAggregate{}, err
		}
	}
	return acc.finish(c.entries.deletes), nil
}

func (a *accumulator) addManifest(file manifestObject, body []byte, small int64) error {
	spec := file.specID
	version := file.version
	content := file.content
	if file.embedded {
		meta, err := manifestHeader(body)
		if err != nil {
			return err
		}
		version = meta.version
		spec = meta.specID
		content = meta.content
	}
	if version == 0 {
		version = 2
	}
	mf := iceberg.NewManifestFile(version, file.path, int64(len(body)), spec, 0).
		Content(content).
		SequenceNum(1, 1).
		Build()
	reader, err := iceberg.NewManifestReader(mf, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("manifest entries: %w", err)
	}
	defer reader.Close()
	for {
		entry, err := reader.ReadEntry()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("manifest entries: %w", err)
		}
		a.addEntry(entry, content, spec, small)
	}
}

func (a *accumulator) addEntry(entry iceberg.ManifestEntry, content iceberg.ManifestContent, spec int32, small int64) {
	switch entry.Status() {
	case iceberg.EntryStatusADDED, iceberg.EntryStatusEXISTING:
	default:
		return
	}
	df := entry.DataFile()
	size := df.FileSizeBytes()
	if content == iceberg.ManifestContentDeletes {
		switch df.ContentType() {
		case iceberg.EntryContentPosDeletes:
			a.position += size
		case iceberg.EntryContentEqDeletes:
			a.equality += size
		}
		return
	}
	a.files++
	a.count++
	a.bytes += size
	if size < small {
		a.small++
	}
	for i, bound := range fileSizeBucketBounds {
		if float64(size) <= bound {
			a.buckets[i]++
		}
	}
	key := partitionKey(spec, df.Partition())
	part := a.parts[key]
	if part == nil {
		part = &partBytes{}
		a.parts[key] = part
	}
	part.files++
	part.bytes += size
	part.records += df.Count()
}

func (a *accumulator) finish(deletes bool) fileAggregate {
	var maxBytes int64
	var totalBytes int64
	for _, part := range a.parts {
		if part.files == 0 && part.records == 0 {
			continue
		}
		totalBytes += part.bytes
		if part.bytes > maxBytes {
			maxBytes = part.bytes
		}
	}
	skew := 0.0
	if len(a.parts) > 0 && totalBytes > 0 {
		skew = float64(maxBytes) * float64(len(a.parts)) / float64(totalBytes)
	}
	agg := fileAggregate{
		count:         a.count,
		sum:           float64(a.bytes),
		buckets:       slices.Clone(a.buckets),
		files:         a.files,
		small:         a.small,
		partitions:    len(a.parts),
		maxBytes:      maxBytes,
		skew:          skew,
		positionBytes: a.position,
		equalityBytes: a.equality,
		deletes:       deletes,
	}
	a.parts = nil
	return agg
}

func emptyAggregate(deletes bool, at time.Time) fileAggregate {
	return fileAggregate{
		buckets: make([]uint64, len(fileSizeBucketBounds)),
		deletes: deletes,
		at:      at,
	}
}

func partitionKey(spec int32, parts map[int]any) string {
	ids := make([]int, 0, len(parts))
	for id := range parts {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	var b strings.Builder
	b.WriteString(strconv.FormatInt(int64(spec), 10))
	for _, id := range ids {
		b.WriteByte('|')
		b.WriteString(strconv.Itoa(id))
		b.WriteByte('=')
		b.WriteString(partitionValue(parts[id]))
	}
	return b.String()
}

func partitionValue(v any) string {
	switch n := v.(type) {
	case nil:
		return "null"
	case string:
		return n
	case int:
		return strconv.Itoa(n)
	case int32:
		return strconv.FormatInt(int64(n), 10)
	case int64:
		return strconv.FormatInt(n, 10)
	case float32:
		return strconv.FormatFloat(float64(n), 'g', -1, 32)
	case float64:
		return strconv.FormatFloat(n, 'g', -1, 64)
	case bool:
		return strconv.FormatBool(n)
	default:
		return fmt.Sprint(n)
	}
}

type manifestHeaderInfo struct {
	version int
	specID  int32
	content iceberg.ManifestContent
}

func manifestHeader(body []byte) (manifestHeaderInfo, error) {
	rd, err := ocf.NewReader(bytes.NewReader(body))
	if err != nil {
		return manifestHeaderInfo{}, fmt.Errorf("manifest entries: avro header: %w", err)
	}
	meta := rd.Metadata()
	info := manifestHeaderInfo{version: 1, content: iceberg.ManifestContentData}
	if raw := meta["format-version"]; len(raw) > 0 {
		info.version, err = strconv.Atoi(string(raw))
		if err != nil {
			return manifestHeaderInfo{}, fmt.Errorf("manifest entries: format-version: %w", err)
		}
	}
	if raw := meta["partition-spec-id"]; len(raw) > 0 {
		spec, err := strconv.Atoi(string(raw))
		if err != nil {
			return manifestHeaderInfo{}, fmt.Errorf("manifest entries: partition-spec-id: %w", err)
		}
		info.specID = int32(spec)
	}
	switch string(meta["content"]) {
	case "", "data":
		info.content = iceberg.ManifestContentData
	case "deletes":
		info.content = iceberg.ManifestContentDeletes
	default:
		return manifestHeaderInfo{}, fmt.Errorf("manifest entries: content %q", meta["content"])
	}
	return info, nil
}

func (c *Collector) emitEntries(ch chan<- prometheus.Metric, labels []string, st entryState, now time.Time) {
	if st.failed || st.ok {
		value := 0.0
		if st.failed {
			value = 1
		}
		ch <- prometheus.MustNewConstMetric(
			c.descs.scrapeError, prometheus.GaugeValue, value,
			append(append([]string{}, labels...), StageManifestEntries)...,
		)
	}
	if !st.ok {
		return
	}
	agg := st.agg
	ch <- prometheus.MustNewConstHistogram(
		c.descs.fileSizeHist, agg.count, agg.sum, bucketMap(agg.buckets), labels...,
	)
	ratio := 0.0
	if agg.files > 0 {
		ratio = float64(agg.small) / float64(agg.files)
	}
	ch <- prometheus.MustNewConstMetric(c.descs.smallRatio, prometheus.GaugeValue, ratio, labels...)
	ch <- prometheus.MustNewConstMetric(c.descs.partitions, prometheus.GaugeValue, float64(agg.partitions), labels...)
	ch <- prometheus.MustNewConstMetric(c.descs.partitionMax, prometheus.GaugeValue, float64(agg.maxBytes), labels...)
	ch <- prometheus.MustNewConstMetric(c.descs.partitionSkew, prometheus.GaugeValue, agg.skew, labels...)
	if agg.deletes {
		ch <- prometheus.MustNewConstMetric(
			c.descs.deleteBytes, prometheus.GaugeValue, float64(agg.positionBytes),
			append(append([]string{}, labels...), deleteContentPosition)...,
		)
		ch <- prometheus.MustNewConstMetric(
			c.descs.deleteBytes, prometheus.GaugeValue, float64(agg.equalityBytes),
			append(append([]string{}, labels...), deleteContentEquality)...,
		)
	}
	age := now.Sub(agg.at).Seconds()
	if age < 0 {
		age = 0
	}
	ch <- prometheus.MustNewConstMetric(c.descs.manifestCacheAge, prometheus.GaugeValue, age, labels...)
}

func bucketMap(cumulative []uint64) map[float64]uint64 {
	out := make(map[float64]uint64, len(fileSizeBucketBounds))
	for i, bound := range fileSizeBucketBounds {
		var n uint64
		if i < len(cumulative) {
			n = cumulative[i]
		}
		out[bound] = n
	}
	return out
}

// Option configures a Collector.
type Option func(*Collector)
