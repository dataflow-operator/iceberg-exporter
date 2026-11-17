package exporter

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"bytes"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"

	"iceberg-exporter/internal/catalog"
	"iceberg-exporter/internal/metadata"
)

// DefaultInterval is how often Run probes catalogs when Config.Interval is zero.
// The Prometheus scrape does not wait for this loop.
const DefaultInterval = 60 * time.Second

// Config is the background catalog walk. Zero intervals keep the catalog
// package defaults: revisioned catalogs check the token every Refresh, and
// other catalogs list on every Refresh.
type Config struct {
	Interval          time.Duration
	ListInterval      time.Duration
	ReconcileInterval time.Duration
	Namespaces        catalog.NamespaceSelector
	PageSize          int
	// Now is the clock for refresh policy and for ages published on scrape.
	// Nil uses time.Now.
	Now func() time.Time
}

// Exporter publishes catalog gauges and the metadata collector on one registry.
// Refresh updates the cache. Handler serves that cache.
type Exporter struct {
	sources  []*source
	meta     *metadata.Collector
	reg      *prometheus.Registry
	descs    catalogDescs
	interval time.Duration
	policy   Config
	now      func() time.Time

	refreshMu sync.Mutex
	snap      atomic.Pointer[snapshot]

	exposition atomic.Pointer[exposition]
	servedGen  atomic.Uint64
	credits    atomic.Pointer[creditBag]
	gen        atomic.Uint64

	handlerOnce sync.Once
	handler     http.Handler
}

// exposition is one rendered /metrics body. gen increases every successful render.
// credits are the commit gauges included in body. Serving this body subtracts
// them once, and only while it is still the latest body.
type exposition struct {
	body    []byte
	gen     uint64
	credits map[*commitGauge]float64
}

type creditBag struct {
	mu    sync.Mutex
	items map[*commitGauge]float64
}

func (b *creditBag) set(g *commitGauge, n float64) {
	if b == nil || g == nil {
		return
	}
	b.mu.Lock()
	if b.items == nil {
		b.items = make(map[*commitGauge]float64)
	}
	b.items[g] = n
	b.mu.Unlock()
}

type source struct {
	cat    catalog.Catalog
	state  *catalog.State
	probed bool
	up     float64
	refs   map[string]refSnap
}

type refSnap struct {
	ref           catalog.Ref
	token         string
	revisioned    bool
	checkedAt     time.Time
	listedAt      time.Time
	tables        []catalog.TablePointer
	views         []catalog.TablePointer
	head          time.Time
	headKnown     bool
	commits       *commitGauge
	diverged      int
	divergedKnown bool
	base          string
}

type snapshot struct {
	catalogs []catalogSnap
}

type catalogSnap struct {
	name   string
	typ    string
	probed bool
	up     float64
	refs   []refSnap
}

// New registers catalogs and the metadata collector on a private registry.
// meta may be nil when every pointer already carries metadata JSON and no
// manifest-entry scan is required. Catalog names are the Prometheus catalog
// label and must be unique.
func New(cfg Config, meta *metadata.Collector, cats ...catalog.Catalog) (*Exporter, error) {
	if len(cats) == 0 {
		return nil, errors.New("exporter: no catalogs")
	}
	if meta == nil {
		meta = metadata.NewCollector(nil)
	}
	sources := make([]*source, 0, len(cats))
	seen := make(map[string]struct{}, len(cats))
	for _, cat := range cats {
		if cat == nil {
			return nil, errors.New("exporter: nil catalog")
		}
		name := cat.Name()
		if name == "" {
			return nil, errors.New("exporter: empty catalog name")
		}
		if _, ok := seen[name]; ok {
			return nil, fmt.Errorf("exporter: duplicate catalog %q", name)
		}
		seen[name] = struct{}{}
		sources = append(sources, &source{
			cat:   cat,
			state: catalog.NewState(),
			refs:  make(map[string]refSnap),
		})
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	interval := cfg.Interval
	if interval <= 0 {
		interval = DefaultInterval
	}
	e := &Exporter{
		sources:  sources,
		meta:     meta,
		reg:      prometheus.NewRegistry(),
		descs:    newCatalogDescs(),
		interval: interval,
		policy:   cfg,
		now:      now,
	}
	e.reg.MustRegister(e, meta)
	return e, nil
}

// Run probes and refreshes until ctx is cancelled.
// The first refresh runs immediately. Later refreshes wait Interval.
func (e *Exporter) Run(ctx context.Context) error {
	if err := e.Refresh(ctx); err != nil {
		return err
	}
	ticker := time.NewTicker(e.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := e.Refresh(ctx); err != nil {
				return err
			}
		}
	}
}

// Refresh probes every catalog and loads table metadata for the pointers
// currently published. A catalog or table error is recorded on that series.
// The error return is only ctx being cancelled.
func (e *Exporter) Refresh(ctx context.Context) error {
	e.refreshMu.Lock()
	defer e.refreshMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	now := e.now()
	var tables []metadata.Table
	for _, src := range e.sources {
		if err := ctx.Err(); err != nil {
			return err
		}
		got, err := e.refreshSource(ctx, src, now)
		tables = append(tables, got...)
		e.publish()
		if err != nil {
			return err
		}
	}
	if err := e.meta.Update(ctx, tables); err != nil {
		return err
	}
	e.render()
	return nil
}

func (e *Exporter) refreshSource(ctx context.Context, src *source, now time.Time) ([]metadata.Table, error) {
	if err := src.cat.Probe(ctx); err != nil {
		if ctx.Err() != nil {
			return src.tableSeries(), ctx.Err()
		}
		src.probed = true
		src.up = 0
		return src.tableSeries(), nil
	}
	results, err := catalog.RefreshAll(ctx, src.cat, src.state, e.listPolicy(now))
	if ctx.Err() != nil {
		return src.tableSeries(), ctx.Err()
	}
	_, revisioned := src.cat.(catalog.Revisioned)
	var hist catalog.Historian
	if h, ok := src.cat.(catalog.Historian); ok {
		hist = h
	}
	base, _ := baseRefName(src.cat)
	src.apply(ctx, results, err == nil, revisioned, hist, base)
	src.probed = true
	src.up = 1
	if ctx.Err() != nil {
		return src.tableSeries(), ctx.Err()
	}
	return src.tableSeries(), nil
}

func (e *Exporter) listPolicy(now time.Time) catalog.Policy {
	return catalog.Policy{
		Now:               now,
		ListInterval:      e.policy.ListInterval,
		ReconcileInterval: e.policy.ReconcileInterval,
		Namespaces:        e.policy.Namespaces,
		PageSize:          e.policy.PageSize,
	}
}

func (s *source) apply(ctx context.Context, results []catalog.Result, complete, revisioned bool, hist catalog.Historian, base string) {
	next := s.refs
	if complete {
		next = make(map[string]refSnap, len(results))
	} else if next == nil {
		next = make(map[string]refSnap)
	}
	for _, res := range results {
		id := refID(res.Ref)
		prev := s.refs[id]
		snap := refSnap{
			ref:        res.Ref,
			token:      res.Revision.Token,
			revisioned: revisioned,
			checkedAt:  res.CheckedAt,
			listedAt:   res.ListedAt,
			tables:     clonePointers(res.Tables),
			views:      clonePointers(res.Views),
			commits:    prev.commits,
		}
		if hist != nil && snap.commits == nil {
			snap.commits = &commitGauge{}
		}
		fillHead(ctx, hist, res.Revision, prev, &snap)
		if res.BaseCompared {
			snap.divergedKnown = true
			snap.diverged = len(res.DivergedFromBase)
			snap.base = base
		}
		next[id] = snap
	}
	s.refs = next
}

func fillHead(ctx context.Context, hist catalog.Historian, rev catalog.Revision, prev refSnap, snap *refSnap) {
	if !rev.CommitTime.IsZero() {
		snap.head = rev.CommitTime
		snap.headKnown = true
	}
	if hist == nil {
		if !snap.headKnown && prev.headKnown && prev.token != "" && prev.token == rev.Token {
			snap.head = prev.head
			snap.headKnown = true
		}
		return
	}
	if rev.Token != "" && rev.Token == prev.token && prev.headKnown && prev.commits.known() {
		snap.head = prev.head
		snap.headKnown = true
		return
	}
	if ctx.Err() != nil {
		keepHistory(prev, snap)
		return
	}
	known := ""
	if prev.token != "" && prev.token != rev.Token {
		known = prev.token
	}
	log, err := hist.History(ctx, snap.ref, known)
	if err != nil {
		keepHistory(prev, snap)
		return
	}
	snap.commits.add(float64(log.NewCommits))
	if !log.Head.CommitTime.IsZero() {
		snap.head = log.Head.CommitTime
		snap.headKnown = true
	} else if !snap.headKnown && prev.headKnown {
		snap.head = prev.head
		snap.headKnown = true
	}
}

func keepHistory(prev refSnap, snap *refSnap) {
	if !snap.headKnown && prev.headKnown {
		snap.head = prev.head
		snap.headKnown = true
	}
}

// commitGauge is the number of new revision tokens not yet served.
// Collect peeks the value. Handler subtracts it once when that exposition is served.
type commitGauge struct {
	mu    sync.Mutex
	value float64
	ready bool
}

func (g *commitGauge) add(n float64) {
	if g == nil {
		return
	}
	g.mu.Lock()
	g.ready = true
	g.value += n
	g.mu.Unlock()
}

func (g *commitGauge) known() bool {
	if g == nil {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.ready
}

func (g *commitGauge) peek() (float64, bool) {
	if g == nil {
		return 0, false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.ready {
		return 0, false
	}
	return g.value, true
}

func (g *commitGauge) subtract(n float64) {
	if g == nil || n == 0 {
		return
	}
	g.mu.Lock()
	g.value -= n
	if g.value < 0 {
		g.value = 0
	}
	g.mu.Unlock()
}

func (s *source) tableSeries() []metadata.Table {
	var out []metadata.Table
	name := s.cat.Name()
	for _, ref := range s.refs {
		for _, p := range ref.tables {
			out = append(out, metadata.Table{
				Catalog: name,
				Ref:     ref.ref.Name,
				Pointer: p,
			})
		}
	}
	return out
}

func (e *Exporter) publish() {
	snap := &snapshot{catalogs: make([]catalogSnap, 0, len(e.sources))}
	for _, src := range e.sources {
		item := catalogSnap{
			name:   src.cat.Name(),
			typ:    src.cat.Type(),
			probed: src.probed,
			up:     src.up,
			refs:   make([]refSnap, 0, len(src.refs)),
		}
		for _, ref := range src.refs {
			item.refs = append(item.refs, ref)
		}
		slices.SortFunc(item.refs, func(a, b refSnap) int {
			if a.ref.Name < b.ref.Name {
				return -1
			}
			if a.ref.Name > b.ref.Name {
				return 1
			}
			if a.ref.Kind < b.ref.Kind {
				return -1
			}
			if a.ref.Kind > b.ref.Kind {
				return 1
			}
			return 0
		})
		snap.catalogs = append(snap.catalogs, item)
	}
	e.snap.Store(snap)
}

// Handler is GET /metrics. It writes the last rendered exposition and does not
// probe the catalog, read the warehouse, or gather metrics. Before the first
// successful refresh it returns 503 with an empty body. A 200 with no series
// would mark the previous scrape stale.
func (e *Exporter) Handler() http.Handler {
	e.handlerOnce.Do(func() {
		e.handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", string(expfmt.FmtText))
			body := e.exposition.Load()
			if body == nil {
				http.Error(w, "metrics are not ready\n", http.StatusServiceUnavailable)
				return
			}
			if _, err := w.Write(body.body); err != nil {
				return
			}
			e.settle(body)
		})
	})
	return e.handler
}

// render gathers the registry into the exposition served by Handler.
// QueueStale runs here so a scrape does not take the collector lock.
// A gather error keeps the previous body.
func (e *Exporter) render() {
	bag := &creditBag{}
	e.credits.Store(bag)
	defer e.credits.Store(nil)
	e.meta.QueueStale(e.now())
	families, err := e.reg.Gather()
	if err != nil {
		return
	}
	var buf bytes.Buffer
	enc := expfmt.NewEncoder(&buf, expfmt.FmtText)
	for _, family := range families {
		if err := enc.Encode(family); err != nil {
			return
		}
	}
	bag.mu.Lock()
	credits := bag.items
	bag.mu.Unlock()
	e.exposition.Store(&exposition{
		body:    buf.Bytes(),
		gen:     e.gen.Add(1),
		credits: credits,
	})
}

// settle drops the commit counts that this body already reported.
// An older body, or a second write of the same body, does not subtract again.
// Counts that arrived after the render stay on the gauge.
func (e *Exporter) settle(body *exposition) {
	for {
		if e.exposition.Load() != body {
			return
		}
		old := e.servedGen.Load()
		if old >= body.gen {
			return
		}
		if !e.servedGen.CompareAndSwap(old, body.gen) {
			continue
		}
		if e.exposition.Load() != body {
			return
		}
		for gauge, n := range body.credits {
			gauge.subtract(n)
		}
		return
	}
}

// Describe implements prometheus.Collector.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	e.descs.describe(ch)
}

// Collect implements prometheus.Collector.
// Ages are computed from the last refresh. Collect does not call the catalog.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	snap := e.snap.Load()
	if snap == nil {
		return
	}
	now := e.now()
	for _, cat := range snap.catalogs {
		if !cat.probed {
			continue
		}
		ch <- prometheus.MustNewConstMetric(e.descs.up, prometheus.GaugeValue, cat.up, cat.name, cat.typ)
		for _, ref := range cat.refs {
			labels := []string{cat.name, ref.ref.Name}
			ch <- prometheus.MustNewConstMetric(e.descs.tables, prometheus.GaugeValue, float64(len(ref.tables)), labels...)
			ch <- prometheus.MustNewConstMetric(e.descs.views, prometheus.GaugeValue, float64(len(ref.views)), labels...)
			ch <- prometheus.MustNewConstMetric(e.descs.namespaces, prometheus.GaugeValue, float64(ref.namespaceCount()), labels...)
			if stamp := ref.listStamp(); !stamp.IsZero() {
				ch <- prometheus.MustNewConstMetric(e.descs.listAge, prometheus.GaugeValue, ageSeconds(stamp, now), labels...)
			}
			if ref.headKnown {
				ch <- prometheus.MustNewConstMetric(e.descs.headAge, prometheus.GaugeValue, ageSeconds(ref.head, now), labels...)
			}
			if n, ok := ref.commits.peek(); ok {
				e.credits.Load().set(ref.commits, n)
				ch <- prometheus.MustNewConstMetric(e.descs.commits, prometheus.GaugeValue, n, labels...)
			}
			if ref.divergedKnown {
				ch <- prometheus.MustNewConstMetric(
					e.descs.diverged, prometheus.GaugeValue, float64(ref.diverged),
					cat.name, ref.ref.Name, ref.base,
				)
			}
		}
	}
}

func (r refSnap) listStamp() time.Time {
	if r.revisioned {
		return r.checkedAt
	}
	return r.listedAt
}

func (r refSnap) namespaceCount() int {
	seen := make(map[string]struct{}, len(r.tables)+len(r.views))
	for _, p := range r.tables {
		seen[p.Namespace] = struct{}{}
	}
	for _, p := range r.views {
		seen[p.Namespace] = struct{}{}
	}
	return len(seen)
}

func ageSeconds(t, now time.Time) float64 {
	if t.IsZero() {
		return 0
	}
	age := now.Sub(t).Seconds()
	if age < 0 {
		return 0
	}
	return age
}

func baseRefName(cat catalog.Catalog) (string, bool) {
	multi, ok := cat.(catalog.MultiRef)
	if !ok {
		return "", false
	}
	ref, ok := multi.BaseRef()
	if !ok || ref.Name == "" {
		return "", false
	}
	return ref.Name, true
}

func refID(ref catalog.Ref) string {
	return string(ref.Kind) + "\x00" + ref.Name
}

func clonePointers(in []catalog.TablePointer) []catalog.TablePointer {
	if len(in) == 0 {
		return nil
	}
	out := make([]catalog.TablePointer, len(in))
	copy(out, in)
	return out
}
