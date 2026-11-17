package exporter

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	"iceberg-exporter/internal/catalog"
	"iceberg-exporter/internal/metadata"
)

func TestHandlerServesCacheWhileRefreshIsBlocked(t *testing.T) {
	t.Parallel()
	cat := &catBase{
		name: "prod",
		typ:  "rest",
		refs: []catalog.Ref{{Name: "main", Kind: catalog.RefSynthetic}},
		tables: map[string][]catalog.TablePointer{
			"main": {tablePtr("db", "events", "id-1")},
		},
	}
	now := time.Unix(1_700_000_000, 0).UTC()
	exp := newExp(t, Config{
		ListInterval: time.Hour,
		Now:          func() time.Time { return now },
	}, cat)
	if err := exp.Refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if got := cat.lists.Load(); got != 1 {
		t.Fatalf("lists = %d, want 1", got)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	cat.probeFn = func(ctx context.Context) error {
		select {
		case <-started:
		default:
			close(started)
		}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	refreshDone := make(chan error, 1)
	go func() {
		refreshDone <- exp.Refresh(context.Background())
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("refresh did not reach the probe")
	}

	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		exp.Handler().ServeHTTP(rec, req)
		done <- rec
	}()
	var rec *httptest.ResponseRecorder
	select {
	case rec = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("metrics handler blocked on catalog refresh")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
	if got := cat.lists.Load(); got != 1 {
		t.Fatalf("scrape listed the catalog, lists = %d", got)
	}
	body := parseBody(t, rec.Body)
	requireGauge(t, body, "iceberg_catalog_up{catalog=prod,type=rest}", 1)
	requireGauge(t, body, "iceberg_tables{catalog=prod,ref=main}", 1)
	requireGauge(t, body, "iceberg_table_has_snapshot{catalog=prod,namespace=db,ref=main,table=events}", 0)
	requireGauge(t, body, "iceberg_exporter_manifest_queue{}", 0)
	if !strings.Contains(helpOf(t, rec.Body.String(), "iceberg_exporter_manifest_queue"), "cache") {
		t.Fatal("manifest queue help does not say the series is cached")
	}

	close(release)
	select {
	case err := <-refreshDone:
		if err != nil {
			t.Fatalf("blocked refresh: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("refresh did not finish")
	}
}

func TestHandlerEmptyBeforeRefresh(t *testing.T) {
	t.Parallel()
	cat := &catBase{
		name: "prod",
		typ:  "rest",
		refs: []catalog.Ref{{Name: "main", Kind: catalog.RefSynthetic}},
	}
	exp := newExp(t, Config{}, cat)
	rec := scrape(t, exp)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "not ready") {
		t.Fatalf("body %q", rec.Body.String())
	}
}

func TestHandlerServesPreviousBodyWhileMetadataReadBlocks(t *testing.T) {
	t.Parallel()
	cat := &catBase{
		name: "prod",
		typ:  "rest",
		refs: []catalog.Ref{{Name: "main", Kind: catalog.RefSynthetic}},
		tables: map[string][]catalog.TablePointer{
			"main": {tablePtr("db", "events", "id-1")},
		},
	}
	started := make(chan struct{})
	release := make(chan struct{})
	reader := &gateReader{started: started, release: release, body: emptyMetadata}
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	meta := metadata.NewCollector(reader)
	t.Cleanup(meta.Close)
	exp, err := New(Config{}, meta, cat)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if err := exp.Refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	cat.mu.Lock()
	next := tablePtr("db", "events", "id-1")
	next.Metadata = nil
	next.MetadataLocation = "s3://wh/db/events/metadata/v2.json"
	cat.tables["main"] = []catalog.TablePointer{next}
	cat.mu.Unlock()

	refreshDone := make(chan error, 1)
	go func() {
		refreshDone <- exp.Refresh(context.Background())
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("refresh did not read metadata")
	}
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		done <- scrape(t, exp)
	}()
	var rec *httptest.ResponseRecorder
	select {
	case rec = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("metrics handler blocked on metadata read")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	body := parseBody(t, rec.Body)
	requireGauge(t, body, "iceberg_table_has_snapshot{catalog=prod,namespace=db,ref=main,table=events}", 0)
	close(release)
	select {
	case err := <-refreshDone:
		if err != nil {
			t.Fatalf("blocked refresh: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("refresh did not finish")
	}
}

func TestTableErrorDoesNotFailScrape(t *testing.T) {
	t.Parallel()
	good := tablePtr("db", "events", "good")
	good.Metadata = emptyMetadata
	bad := tablePtr("db", "broken", "bad")
	bad.Metadata = []byte(`{`)
	bad.MetadataLocation = "s3://wh/db/broken/metadata/v1.json"
	cat := &catBase{
		name: "prod",
		typ:  "nessie",
		refs: []catalog.Ref{{Name: "main", Kind: catalog.RefBranch}},
		tables: map[string][]catalog.TablePointer{
			"main": {good, bad},
		},
	}
	exp := newExp(t, Config{}, cat)
	if err := exp.Refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	rec := httptest.NewRecorder()
	exp.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	got := parseBody(t, rec.Body)
	requireGauge(t, got, "iceberg_tables{catalog=prod,ref=main}", 2)
	requireGauge(t, got, "iceberg_table_has_snapshot{catalog=prod,namespace=db,ref=main,table=events}", 0)
	requireGauge(t, got, "iceberg_table_scrape_error{catalog=prod,namespace=db,ref=main,stage=metadata,table=events}", 0)
	requireGauge(t, got, "iceberg_table_scrape_error{catalog=prod,namespace=db,ref=main,stage=metadata,table=broken}", 1)
	absent(t, got, "iceberg_table_has_snapshot{catalog=prod,namespace=db,ref=main,table=broken}")
}

func TestRevisionHeadAgeAndCommitsSkipUnchangedHash(t *testing.T) {
	t.Parallel()
	var token atomic.Value
	token.Store("aaa")
	head := time.Unix(1_700_000_000, 0).UTC()
	base := &catBase{
		name: "prod",
		typ:  "nessie",
		refs: []catalog.Ref{{Name: "main", Kind: catalog.RefBranch}},
		tables: map[string][]catalog.TablePointer{
			"main": {tablePtr("db", "events", "id-1")},
		},
	}
	cat := &histCat{revCat: &revCat{
		catBase: base,
		revision: func(catalog.Ref) (catalog.Revision, error) {
			return catalog.Revision{Token: token.Load().(string)}, nil
		},
	}}
	cat.history = func(_ catalog.Ref, known string) (catalog.CommitLog, error) {
		cat.mu.Lock()
		cat.known = append(cat.known, known)
		cat.mu.Unlock()
		commits := 0
		if known == "aaa" {
			commits = 3
		}
		return catalog.CommitLog{
			Head:       catalog.Revision{Token: token.Load().(string), CommitTime: head},
			NewCommits: commits,
		}, nil
	}
	now := head.Add(10 * time.Second)
	exp := newExp(t, Config{Now: func() time.Time { return now }}, cat)
	if err := exp.Refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	got := gauges(t, exp)
	requireGauge(t, got, "iceberg_catalog_commits_since_scrape{catalog=prod,ref=main}", 0)
	requireGauge(t, got, "iceberg_catalog_ref_head_age_seconds{catalog=prod,ref=main}", 10)
	requireGauge(t, got, "iceberg_catalog_list_age_seconds{catalog=prod,ref=main}", 0)
	absent(t, got, "iceberg_ref_tables_diverged{base_ref=main,catalog=prod,ref=main}")
	if join(cat.knownTokens()) != "" {
		t.Fatalf("first history known tokens %q", join(cat.knownTokens()))
	}
	if got := base.lists.Load(); got != 1 {
		t.Fatalf("lists = %d, want 1", got)
	}

	now = now.Add(30 * time.Second)
	if err := exp.Refresh(context.Background()); err != nil {
		t.Fatalf("second refresh: %v", err)
	}
	got = gauges(t, exp)
	requireGauge(t, got, "iceberg_catalog_commits_since_scrape{catalog=prod,ref=main}", 0)
	requireGauge(t, got, "iceberg_catalog_ref_head_age_seconds{catalog=prod,ref=main}", 40)
	requireGauge(t, got, "iceberg_catalog_list_age_seconds{catalog=prod,ref=main}", 0)
	if len(cat.knownTokens()) != 1 {
		t.Fatalf("unchanged hash walked history again: %q", join(cat.knownTokens()))
	}
	if got := base.lists.Load(); got != 1 {
		t.Fatalf("unchanged hash listed again, lists = %d", got)
	}

	token.Store("bbb")
	if err := exp.Refresh(context.Background()); err != nil {
		t.Fatalf("third refresh: %v", err)
	}
	got = gauges(t, exp)
	requireGauge(t, got, "iceberg_catalog_commits_since_scrape{catalog=prod,ref=main}", 3)
	first := scrape(t, exp)
	second := scrape(t, exp)
	requireGauge(t, parseBody(t, first.Body), "iceberg_catalog_commits_since_scrape{catalog=prod,ref=main}", 3)
	requireGauge(t, parseBody(t, second.Body), "iceberg_catalog_commits_since_scrape{catalog=prod,ref=main}", 3)
	if first.Body.String() != second.Body.String() {
		t.Fatal("second scrape rebuilt the exposition")
	}
	got = gauges(t, exp)
	requireGauge(t, got, "iceberg_catalog_commits_since_scrape{catalog=prod,ref=main}", 0)
	if join(cat.knownTokens()) != ",aaa" {
		t.Fatalf("history tokens %q", join(cat.knownTokens()))
	}

	cat.history = func(_ catalog.Ref, known string) (catalog.CommitLog, error) {
		cat.mu.Lock()
		cat.known = append(cat.known, known)
		cat.mu.Unlock()
		return catalog.CommitLog{
			Head:       catalog.Revision{Token: token.Load().(string), CommitTime: head},
			NewCommits: 2,
		}, nil
	}
	token.Store("ccc")
	if err := exp.Refresh(context.Background()); err != nil {
		t.Fatalf("fourth refresh: %v", err)
	}
	token.Store("ddd")
	if err := exp.Refresh(context.Background()); err != nil {
		t.Fatalf("fifth refresh: %v", err)
	}
	got = gauges(t, exp)
	requireGauge(t, got, "iceberg_catalog_commits_since_scrape{catalog=prod,ref=main}", 4)
}

func TestListAgeUsesLastFullList(t *testing.T) {
	t.Parallel()
	cat := &catBase{
		name: "hive",
		typ:  "hive",
		refs: []catalog.Ref{{Name: "main", Kind: catalog.RefSynthetic}},
		tables: map[string][]catalog.TablePointer{
			"main": {tablePtr("db", "events", "id-1")},
		},
	}
	now := time.Unix(1_000, 0).UTC()
	exp := newExp(t, Config{
		ListInterval: time.Hour,
		Now:          func() time.Time { return now },
	}, cat)
	if err := exp.Refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	now = now.Add(30 * time.Second)
	if err := exp.Refresh(context.Background()); err != nil {
		t.Fatalf("cached refresh: %v", err)
	}
	got := gauges(t, exp)
	requireGauge(t, got, "iceberg_catalog_list_age_seconds{catalog=hive,ref=main}", 30)
	absent(t, got, "iceberg_catalog_ref_head_age_seconds{catalog=hive,ref=main}")
	absent(t, got, "iceberg_catalog_commits_since_scrape{catalog=hive,ref=main}")
	absent(t, got, "iceberg_ref_tables_diverged{base_ref=,catalog=hive,ref=main}")
	if lists := cat.lists.Load(); lists != 1 {
		t.Fatalf("lists = %d, want 1", lists)
	}
}

func TestDivergenceNamespacesAndCapabilityAbsence(t *testing.T) {
	t.Parallel()
	mainTable := tablePtr("analytics", "events", "id-1")
	mainTable.SnapshotID = snap(1)
	daily := tablePtr("analytics.daily", "sessions", "id-2")
	daily.SnapshotID = snap(1)
	view := tablePtr("analytics", "events_view", "view-1")
	view.Type = catalog.ContentView
	featureEvents := mainTable
	featureEvents.SnapshotID = snap(2)
	onlyFeature := tablePtr("analytics", "scratch", "id-3")
	onlyFeature.SnapshotID = snap(4)
	base := &catBase{
		name: "prod",
		typ:  "nessie",
		refs: []catalog.Ref{
			{Name: "main", Kind: catalog.RefBranch},
			{Name: "feature", Kind: catalog.RefBranch},
		},
		tables: map[string][]catalog.TablePointer{
			"main":    {mainTable, daily, view},
			"feature": {featureEvents, onlyFeature},
		},
	}
	cat := &multiRev{
		revCat: &revCat{
			catBase: base,
			revision: func(ref catalog.Ref) (catalog.Revision, error) {
				return catalog.Revision{Token: ref.Name, CommitTime: time.Unix(50, 0).UTC()}, nil
			},
		},
		base: catalog.Ref{Name: "main", Kind: catalog.RefBranch},
	}
	now := time.Unix(80, 0).UTC()
	exp := newExp(t, Config{Now: func() time.Time { return now }}, cat)
	if err := exp.Refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	got := gauges(t, exp)
	requireGauge(t, got, "iceberg_tables{catalog=prod,ref=main}", 2)
	requireGauge(t, got, "iceberg_views{catalog=prod,ref=main}", 1)
	requireGauge(t, got, "iceberg_namespaces{catalog=prod,ref=main}", 2)
	requireGauge(t, got, "iceberg_tables{catalog=prod,ref=feature}", 2)
	requireGauge(t, got, "iceberg_views{catalog=prod,ref=feature}", 0)
	requireGauge(t, got, "iceberg_namespaces{catalog=prod,ref=feature}", 1)
	requireGauge(t, got, "iceberg_ref_tables_diverged{base_ref=main,catalog=prod,ref=feature}", 2)
	requireGauge(t, got, "iceberg_catalog_ref_head_age_seconds{catalog=prod,ref=main}", 30)
	absent(t, got, "iceberg_ref_tables_diverged{base_ref=main,catalog=prod,ref=main}")
	absent(t, got, "iceberg_catalog_commits_since_scrape{catalog=prod,ref=main}")
	absent(t, got, "iceberg_table_has_snapshot{catalog=prod,namespace=analytics,ref=main,table=events_view}")
}

func TestProbeFailureKeepsTableMetrics(t *testing.T) {
	t.Parallel()
	cat := &catBase{
		name: "prod",
		typ:  "rest",
		refs: []catalog.Ref{{Name: "main", Kind: catalog.RefSynthetic}},
		tables: map[string][]catalog.TablePointer{
			"main": {tablePtr("db", "events", "id-1")},
		},
	}
	now := time.Unix(1_000, 0).UTC()
	exp := newExp(t, Config{
		ListInterval: time.Hour,
		Now:          func() time.Time { return now },
	}, cat)
	if err := exp.Refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	cat.probeFn = func(context.Context) error { return errors.New("down") }
	cat.listRefsFn = func() error { return errors.New("refs should not be listed") }
	now = now.Add(45 * time.Second)
	if err := exp.Refresh(context.Background()); err != nil {
		t.Fatalf("failed probe: %v", err)
	}
	got := gauges(t, exp)
	requireGauge(t, got, "iceberg_catalog_up{catalog=prod,type=rest}", 0)
	requireGauge(t, got, "iceberg_tables{catalog=prod,ref=main}", 1)
	requireGauge(t, got, "iceberg_catalog_list_age_seconds{catalog=prod,ref=main}", 45)
	requireGauge(t, got, "iceberg_table_has_snapshot{catalog=prod,namespace=db,ref=main,table=events}", 0)
	if lists := cat.lists.Load(); lists != 1 {
		t.Fatalf("lists = %d, want 1", lists)
	}
}

func TestCatalogsStayDistinctWhenOneListFails(t *testing.T) {
	t.Parallel()
	live := &catBase{
		name: "live",
		typ:  "nessie",
		refs: []catalog.Ref{{Name: "main", Kind: catalog.RefBranch}},
		tables: map[string][]catalog.TablePointer{
			"main": {tablePtr("db", "events", "live-1")},
		},
	}
	stuck := &revCat{
		catBase: &catBase{
			name: "stuck",
			typ:  "nessie",
			refs: []catalog.Ref{{Name: "main", Kind: catalog.RefBranch}},
			tables: map[string][]catalog.TablePointer{
				"main": {tablePtr("db", "events", "stuck-1")},
			},
		},
		revision: func(catalog.Ref) (catalog.Revision, error) {
			return catalog.Revision{Token: "aaa", CommitTime: time.Unix(10, 0).UTC()}, nil
		},
	}
	now := time.Unix(20, 0).UTC()
	exp := newExp(t, Config{Now: func() time.Time { return now }}, live, stuck)
	if err := exp.Refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	stuck.listRefsFn = func() error { return errors.New("list refs") }
	now = now.Add(15 * time.Second)
	live.mu.Lock()
	live.tables["main"] = []catalog.TablePointer{tablePtr("db", "events", "live-1"), tablePtr("db", "other", "live-2")}
	live.mu.Unlock()
	if err := exp.Refresh(context.Background()); err != nil {
		t.Fatalf("partial refresh: %v", err)
	}
	got := gauges(t, exp)
	requireGauge(t, got, "iceberg_catalog_up{catalog=live,type=nessie}", 1)
	requireGauge(t, got, "iceberg_catalog_up{catalog=stuck,type=nessie}", 1)
	requireGauge(t, got, "iceberg_tables{catalog=live,ref=main}", 2)
	requireGauge(t, got, "iceberg_tables{catalog=stuck,ref=main}", 1)
	requireGauge(t, got, "iceberg_catalog_list_age_seconds{catalog=stuck,ref=main}", 15)
	requireGauge(t, got, "iceberg_catalog_list_age_seconds{catalog=live,ref=main}", 0)
	requireGauge(t, got, "iceberg_table_records{catalog=live,namespace=db,ref=main,table=other}", 0)
	requireGauge(t, got, "iceberg_table_has_snapshot{catalog=stuck,namespace=db,ref=main,table=events}", 0)
}

func TestRunStopsWhenCancelled(t *testing.T) {
	t.Parallel()
	cat := &catBase{
		name:   "prod",
		typ:    "rest",
		refs:   []catalog.Ref{{Name: "main", Kind: catalog.RefSynthetic}},
		tables: map[string][]catalog.TablePointer{"main": {tablePtr("db", "events", "id-1")}},
	}
	exp := newExp(t, Config{Interval: time.Hour}, cat)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- exp.Run(ctx)
	}()
	deadline := time.Now().Add(2 * time.Second)
	for cat.probes.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("run did not probe")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("run err %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run did not stop")
	}
}

func TestRejectsDuplicateCatalogName(t *testing.T) {
	t.Parallel()
	a := &catBase{name: "prod", typ: "rest"}
	b := &catBase{name: "prod", typ: "hive"}
	if _, err := New(Config{}, nil, a, b); err == nil {
		t.Fatal("duplicate catalog name was accepted")
	}
}

type catBase struct {
	name       string
	typ        string
	refs       []catalog.Ref
	mu         sync.Mutex
	tables     map[string][]catalog.TablePointer
	probeFn    func(context.Context) error
	listRefsFn func() error
	probes     atomic.Int32
	lists      atomic.Int32
}

func (c *catBase) Name() string { return c.name }

func (c *catBase) Type() string { return c.typ }

func (c *catBase) Probe(ctx context.Context) error {
	c.probes.Add(1)
	if c.probeFn != nil {
		return c.probeFn(ctx)
	}
	return nil
}

func (c *catBase) ListRefs(context.Context) ([]catalog.Ref, error) {
	if c.listRefsFn != nil {
		if err := c.listRefsFn(); err != nil {
			return nil, err
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]catalog.Ref, len(c.refs))
	copy(out, c.refs)
	return out, nil
}

func (c *catBase) ListTables(_ context.Context, ref catalog.Ref, _ catalog.ListOptions) (catalog.TablePage, error) {
	c.lists.Add(1)
	c.mu.Lock()
	defer c.mu.Unlock()
	src := c.tables[ref.Name]
	out := make([]catalog.TablePointer, len(src))
	copy(out, src)
	return catalog.TablePage{Tables: out}, nil
}

type revCat struct {
	*catBase
	revision func(catalog.Ref) (catalog.Revision, error)
}

func (c *revCat) Revision(_ context.Context, ref catalog.Ref) (catalog.Revision, error) {
	return c.revision(ref)
}

type histCat struct {
	*revCat
	mu      sync.Mutex
	history func(catalog.Ref, string) (catalog.CommitLog, error)
	known   []string
}

func (c *histCat) History(_ context.Context, ref catalog.Ref, known string) (catalog.CommitLog, error) {
	return c.history(ref, known)
}

func (c *histCat) knownTokens() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.known))
	copy(out, c.known)
	return out
}

type multiRev struct {
	*revCat
	base catalog.Ref
}

func (m *multiRev) BaseRef() (catalog.Ref, bool) {
	if m.base.Name == "" {
		return catalog.Ref{}, false
	}
	return m.base, true
}

func newExp(t *testing.T, cfg Config, cats ...catalog.Catalog) *Exporter {
	t.Helper()
	meta := metadata.NewCollector(nil)
	t.Cleanup(meta.Close)
	exp, err := New(cfg, meta, cats...)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	return exp
}

func tablePtr(namespace, name, id string) catalog.TablePointer {
	return catalog.TablePointer{
		ContentID:        id,
		Namespace:        namespace,
		Name:             name,
		MetadataLocation: "s3://wh/" + namespace + "/" + name + "/metadata/v1.json",
		Metadata:         emptyMetadata,
		Type:             catalog.ContentTable,
	}
}

func snap(id int64) *int64 { return &id }

type gateReader struct {
	started chan struct{}
	release chan struct{}
	body    []byte
	once    sync.Once
}

func (g *gateReader) Read(ctx context.Context, _ string) ([]byte, error) {
	g.once.Do(func() { close(g.started) })
	select {
	case <-g.release:
		return g.body, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func scrape(t *testing.T, exp *Exporter) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	exp.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	return rec
}

func gauges(t *testing.T, exp *Exporter) map[string]float64 {
	t.Helper()
	families, err := exp.reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	return familyGauges(families)
}

func parseBody(t *testing.T, body *bytes.Buffer) map[string]float64 {
	t.Helper()
	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(bytes.NewReader(body.Bytes()))
	if err != nil {
		t.Fatalf("parse metrics: %v", err)
	}
	list := make([]*dto.MetricFamily, 0, len(families))
	for _, fam := range families {
		list = append(list, fam)
	}
	return familyGauges(list)
}

func familyGauges(families []*dto.MetricFamily) map[string]float64 {
	out := make(map[string]float64)
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			if metric.Gauge == nil {
				continue
			}
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

func absent(t *testing.T, got map[string]float64, name string) {
	t.Helper()
	if _, ok := got[name]; ok {
		t.Fatalf("%s was published", name)
	}
}

func helpOf(t *testing.T, body, name string) string {
	t.Helper()
	for line := range strings.SplitSeq(body, "\n") {
		if strings.HasPrefix(line, "# HELP "+name+" ") {
			return line
		}
	}
	t.Fatalf("missing help for %s", name)
	return ""
}

func join(parts []string) string {
	return strings.Join(parts, ",")
}

var emptyMetadata = []byte(`{
	"format-version": 2,
	"table-uuid": "11111111-1111-4111-8111-111111111111",
	"location": "s3://wh/db/events",
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
	"metadata-log": []
}`)
