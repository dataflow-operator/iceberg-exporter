package nessie

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"iceberg-exporter/internal/catalog"
)

func TestNewRejectsBadConfig(t *testing.T) {
	t.Parallel()
	refs := []catalog.Ref{{Name: "main", Kind: catalog.RefBranch}}
	cases := []struct {
		name   string
		cfg    Config
		want   string
		secret string
	}{
		{name: "name", cfg: Config{BaseURL: "http://nessie", Refs: refs}, want: "catalog name"},
		{name: "url", cfg: Config{Name: "wh", Refs: refs}, want: "base URL"},
		{name: "scheme", cfg: Config{Name: "wh", BaseURL: "ftp://nessie", Refs: refs}, want: "scheme"},
		{name: "userinfo", cfg: Config{Name: "wh", BaseURL: "http://user:secret@nessie", Refs: refs}, want: "userinfo", secret: "secret"},
		{name: "query", cfg: Config{Name: "wh", BaseURL: "http://nessie?token=secret", Refs: refs}, want: "query", secret: "secret"},
		{name: "refs", cfg: Config{Name: "wh", BaseURL: "http://nessie"}, want: "no refs"},
		{name: "kind", cfg: Config{Name: "wh", BaseURL: "http://nessie", Refs: []catalog.Ref{{Name: "main", Kind: catalog.RefSynthetic}}}, want: "kind"},
		{name: "duplicate", cfg: Config{Name: "wh", BaseURL: "http://nessie", Refs: []catalog.Ref{{Name: "main"}, {Name: "main", Kind: catalog.RefTag}}}, want: "duplicate"},
		{name: "base", cfg: Config{Name: "wh", BaseURL: "http://nessie", Refs: refs, BaseRef: "other"}, want: "base ref"},
		{name: "page", cfg: Config{Name: "wh", BaseURL: "http://nessie", Refs: refs, PageSize: -1}, want: "page size"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := New(tc.cfg)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err %v", err)
			}
			if tc.secret != "" && strings.Contains(err.Error(), tc.secret) {
				t.Fatalf("secret leaked: %v", err)
			}
		})
	}
}

func TestNewNormalizesRefsAndBaseURL(t *testing.T) {
	t.Parallel()
	client, err := New(Config{
		Name:    " warehouse ",
		BaseURL: "http://nessie.internal:19120/api/v2/",
		Refs: []catalog.Ref{
			{Name: " release ", Kind: catalog.RefTag},
			{Name: "main"},
		},
		BaseRef: " main ",
	})
	if err != nil {
		t.Fatal(err)
	}
	if client.Name() != "warehouse" || client.Type() != "nessie" {
		t.Fatalf("name %q type %q", client.Name(), client.Type())
	}
	if client.baseURL != "http://nessie.internal:19120/api/v2" {
		t.Fatalf("base %q", client.baseURL)
	}
	refs, err := client.ListRefs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 2 || refs[0].Name != "release" || refs[0].Kind != catalog.RefTag || refs[1].Kind != catalog.RefBranch {
		t.Fatalf("refs %+v", refs)
	}
	base, ok := client.BaseRef()
	if !ok || base.Name != "main" || base.Kind != catalog.RefBranch {
		t.Fatalf("base %+v ok %v", base, ok)
	}
	bare, err := New(Config{Name: "wh", BaseURL: "https://nessie.example/prefix", Refs: []catalog.Ref{{Name: "main"}}})
	if err != nil {
		t.Fatal(err)
	}
	if bare.baseURL != "https://nessie.example/prefix/api/v2" {
		t.Fatalf("prefixed base %q", bare.baseURL)
	}
	if _, ok := bare.BaseRef(); ok {
		t.Fatal("unset base ref was reported")
	}
}

func TestEntriesFilter(t *testing.T) {
	t.Parallel()
	if got := entriesFilter(catalog.NamespaceSelector{}); got != "entry.contentType=='ICEBERG_TABLE' || entry.contentType=='ICEBERG_VIEW'" {
		t.Fatalf("open filter %q", got)
	}
	sel := catalog.NamespaceSelector{Include: []string{"analytics", "o'brien"}, Exclude: []string{"analytics.secret"}}
	got := entriesFilter(sel)
	want := "(" + "entry.contentType=='ICEBERG_TABLE' || entry.contentType=='ICEBERG_VIEW'" + ") && (" +
		"((entry.namespace == 'analytics' || entry.namespace.startsWith('analytics.')) || " +
		"(entry.namespace == 'o\\'brien' || entry.namespace.startsWith('o\\'brien.'))) && " +
		"!((entry.namespace == 'analytics.secret' || entry.namespace.startsWith('analytics.secret.'))))"
	if got != want {
		t.Fatalf("got %q\nwant %q", got, want)
	}
	root := entriesFilter(catalog.NamespaceSelector{Include: []string{""}})
	if !strings.Contains(root, "entry.namespace == ''") || strings.Contains(root, "startsWith") {
		t.Fatalf("root filter %q", root)
	}
}

func TestDecodeContent(t *testing.T) {
	t.Parallel()
	raw := json.RawMessage(`{"type":"ICEBERG_TABLE","id":"from-body","metadataLocation":"s3://wh/t","snapshotId":0}`)
	p, ok, err := decodeContent([]string{"db", "events"}, "ICEBERG_VIEW", "from-entry", raw)
	if err != nil || !ok {
		t.Fatalf("ok %v err %v", ok, err)
	}
	if p.ContentID != "from-body" || p.Namespace != "db" || p.Name != "events" || p.Type != catalog.ContentTable || p.Metadata != nil {
		t.Fatalf("pointer %+v", p)
	}
	if p.SnapshotID == nil || *p.SnapshotID != 0 {
		t.Fatalf("snapshot %#v", p.SnapshotID)
	}
	view, ok, err := decodeContent([]string{"orders_view"}, "", "", json.RawMessage(`{"type":"ICEBERG_VIEW","id":"v","metadataLocation":"s3://v","versionId":3}`))
	if err != nil || !ok || view.Namespace != "" || view.Type != catalog.ContentView || view.SnapshotID == nil || *view.SnapshotID != 3 {
		t.Fatalf("view %+v ok %v err %v", view, ok, err)
	}
	if _, ok, err := decodeContent([]string{"db", "legacy"}, "", "", json.RawMessage(`{"type":"DELTA_LAKE_TABLE","id":"d"}`)); err != nil || ok {
		t.Fatalf("delta ok %v err %v", ok, err)
	}
	fallback, ok, err := decodeContent([]string{"t"}, "ICEBERG_TABLE", "entry-id", nil)
	if err != nil || !ok || fallback.ContentID != "entry-id" || fallback.SnapshotID != nil || fallback.MetadataLocation != "" {
		t.Fatalf("fallback %+v ok %v err %v", fallback, ok, err)
	}
	if _, _, err := decodeContent(nil, "ICEBERG_TABLE", "id", nil); err == nil {
		t.Fatal("empty key")
	}
	if _, _, err := decodeContent([]string{"db", ""}, "ICEBERG_TABLE", "id", nil); err == nil {
		t.Fatal("empty element")
	}
}

func TestChangesFromRenameDeleteAndSkip(t *testing.T) {
	t.Parallel()
	table := func(id, location string, snapshot int64) json.RawMessage {
		return json.RawMessage(`{"type":"ICEBERG_TABLE","id":"` + id + `","metadataLocation":"` + location + `","snapshotId":` + itoa(snapshot) + `}`)
	}
	changes, err := changesFrom([]diffEntryJSON{
		{Key: contentKeyJSON{Elements: []string{"db", "events"}}, From: table("id-1", "s3://old", 1), To: json.RawMessage(`null`)},
		{Key: contentKeyJSON{Elements: []string{"db", "events2"}}, From: json.RawMessage(`null`), To: table("id-1", "s3://old", 1)},
		{Key: contentKeyJSON{Elements: []string{"db", "gone"}}, From: table("id-2", "s3://gone", 4), To: json.RawMessage(`null`)},
		{Key: contentKeyJSON{Elements: []string{"db", "replaced"}}, From: table("id-3", "s3://a", 1), To: table("id-4", "s3://b", 2)},
		{Key: contentKeyJSON{Elements: []string{"db", "legacy"}}, From: json.RawMessage(`{"type":"DELTA_LAKE_TABLE","id":"d"}`), To: json.RawMessage(`{"type":"NAMESPACE","id":"n"}`)},
		{Key: contentKeyJSON{Elements: []string{"db", "promoted"}}, From: json.RawMessage(`{"type":"DELTA_LAKE_TABLE","id":"d2"}`), To: table("id-5", "s3://new", 1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []catalog.Change{
		{Kind: catalog.ChangeUpsert, Pointer: catalog.TablePointer{ContentID: "id-1", Name: "events2"}},
		{Kind: catalog.ChangeDelete, Pointer: catalog.TablePointer{ContentID: "id-2", Name: "gone"}},
		{Kind: catalog.ChangeDelete, Pointer: catalog.TablePointer{ContentID: "id-3", Name: "replaced"}},
		{Kind: catalog.ChangeUpsert, Pointer: catalog.TablePointer{ContentID: "id-4", Name: "replaced"}},
		{Kind: catalog.ChangeUpsert, Pointer: catalog.TablePointer{ContentID: "id-5", Name: "promoted"}},
	}
	if len(changes) != len(want) {
		t.Fatalf("changes %+v", changes)
	}
	for i, change := range changes {
		if change.Kind != want[i].Kind || change.Pointer.ContentID != want[i].Pointer.ContentID || change.Pointer.Name != want[i].Pointer.Name {
			t.Fatalf("change %d %+v", i, change)
		}
	}
}

func TestClientProbeRevisionAndEntries(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var hits []recorded
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits = append(hits, record(r))
		mu.Unlock()
		if r.URL.Path != "/api/v2/config" && r.Header.Get("Authorization") != "Bearer secret" {
			http.Error(w, "no auth", http.StatusUnauthorized)
			return
		}
		switch r.URL.RequestURI() {
		case "/api/v2/config":
			w.Write([]byte(`{"specVersion":"2.2.0"}`))
		case "/api/v2/trees/main":
			w.Write([]byte(`{"type":"BRANCH","name":"main","hash":"aaa"}`))
		default:
			if r.URL.Path == "/api/v2/trees/main/entries" {
				q := r.URL.Query()
				if q.Get("content") != "true" || q.Get("page-token") != pageTokenOf(r) {
					http.Error(w, "bad query", http.StatusBadRequest)
					return
				}
				if !strings.Contains(q.Get("filter"), "ICEBERG_TABLE") || !strings.Contains(q.Get("filter"), "ICEBERG_VIEW") {
					http.Error(w, "missing content filter", http.StatusBadRequest)
					return
				}
				if strings.Contains(r.URL.Path, "/contents") {
					http.Error(w, "contents", http.StatusBadRequest)
					return
				}
				switch q.Get("page-token") {
				case "":
					if q.Get("max-records") != "1" {
						http.Error(w, "max-records", http.StatusBadRequest)
						return
					}
					w.Write([]byte(`{
						"entries": [
							{"type":"ICEBERG_TABLE","name":{"elements":["sales","events"]},"contentId":"cid-events","content":{"type":"ICEBERG_TABLE","id":"cid-events","metadataLocation":"s3://bucket/sales/events/metadata/00000.metadata.json","snapshotId":42,"schemaId":0,"specId":0,"sortOrderId":0}},
							{"type":"DELTA_LAKE_TABLE","name":{"elements":["sales","legacy"]},"contentId":"cid-delta","content":{"type":"DELTA_LAKE_TABLE","id":"cid-delta"}},
							{"type":"NAMESPACE","name":{"elements":["sales"]},"contentId":"cid-ns","content":{"type":"NAMESPACE","id":"cid-ns"}}
						],
						"token":"page-2",
						"hasMore": true
					}`))
				case "page-2":
					w.Write([]byte(`{
						"entries": [
							{"type":"ICEBERG_VIEW","name":{"elements":["sales","orders_view"]},"contentId":"cid-view","content":{"type":"ICEBERG_VIEW","id":"cid-view","metadataLocation":"s3://bucket/sales/orders_view/metadata/v1.metadata.json","versionId":3,"schemaId":0}}
						],
						"hasMore": false,
						"token": "ignored"
					}`))
				default:
					http.Error(w, "token", http.StatusBadRequest)
				}
				return
			}
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)

	client := mustClient(t, srv, Config{
		Token:    "secret",
		PageSize: 50,
		Refs:     []catalog.Ref{{Name: "main", Kind: catalog.RefBranch}, {Name: "release", Kind: catalog.RefTag}},
		BaseRef:  "main",
	})
	if err := client.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	rev, err := client.Revision(context.Background(), catalog.Ref{Name: "main", Kind: catalog.RefBranch})
	if err != nil {
		t.Fatal(err)
	}
	if rev.Token != "aaa" || !rev.CommitTime.IsZero() {
		t.Fatalf("revision %+v", rev)
	}
	if _, err := client.Revision(context.Background(), catalog.Ref{Name: "main", Kind: catalog.RefBranch}); err != nil {
		t.Fatal(err)
	}
	first, err := client.ListTables(context.Background(), catalog.Ref{Name: "main", Kind: catalog.RefBranch}, catalog.ListOptions{PageSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	if first.NextPageToken != "page-2" || len(first.Tables) != 1 || first.Tables[0].Name != "events" || first.Tables[0].Metadata != nil {
		t.Fatalf("first %+v", first)
	}
	if first.Tables[0].SnapshotID == nil || *first.Tables[0].SnapshotID != 42 || first.Tables[0].ContentID != "cid-events" {
		t.Fatalf("table %+v", first.Tables[0])
	}
	second, err := client.ListTables(context.Background(), catalog.Ref{Name: "main", Kind: catalog.RefBranch}, catalog.ListOptions{PageToken: "page-2", PageSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	if second.NextPageToken != "" || len(second.Tables) != 1 || second.Tables[0].Type != catalog.ContentView || *second.Tables[0].SnapshotID != 3 {
		t.Fatalf("second %+v", second)
	}
	mu.Lock()
	defer mu.Unlock()
	var trees, entries, contents int
	for _, hit := range hits {
		switch {
		case hit.path == "/api/v2/trees/main":
			trees++
		case hit.path == "/api/v2/trees/main/entries":
			entries++
		case strings.Contains(hit.path, "/contents") || strings.Contains(hit.path, "/history"):
			contents++
		}
	}
	if trees != 2 || entries != 2 || contents != 0 {
		t.Fatalf("trees %d entries %d extra %d hits %+v", trees, entries, contents, hits)
	}
}

func TestClientListTablesNamespaceAndRepeatedToken(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		filter := q.Get("filter")
		if !strings.Contains(filter, "entry.namespace == 'analytics'") || !strings.Contains(filter, "analytics.secret") {
			http.Error(w, filter, http.StatusBadRequest)
			return
		}
		if q.Get("page-token") == "loop" {
			w.Write([]byte(`{"entries":[],"hasMore":true,"token":"loop"}`))
			return
		}
		w.Write([]byte(`{
			"entries": [
				{"type":"ICEBERG_TABLE","name":{"elements":["analytics","events"]},"contentId":"in","content":{"type":"ICEBERG_TABLE","id":"in","metadataLocation":"s3://in","snapshotId":1,"schemaId":0,"specId":0,"sortOrderId":0}},
				{"type":"ICEBERG_TABLE","name":{"elements":["ops","users"]},"contentId":"out","content":{"type":"ICEBERG_TABLE","id":"out","metadataLocation":"s3://out","snapshotId":1,"schemaId":0,"specId":0,"sortOrderId":0}}
			],
			"hasMore": false
		}`))
	}))
	t.Cleanup(srv.Close)
	client := mustClient(t, srv, Config{Refs: []catalog.Ref{{Name: "main"}}})
	sel := catalog.NamespaceSelector{Include: []string{"analytics"}, Exclude: []string{"analytics.secret"}}
	page, err := client.ListTables(context.Background(), catalog.Ref{Name: "main"}, catalog.ListOptions{Namespaces: sel})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Tables) != 1 || page.Tables[0].Namespace != "analytics" {
		t.Fatalf("page %+v", page.Tables)
	}
	if _, err := client.ListTables(context.Background(), catalog.Ref{Name: "main"}, catalog.ListOptions{PageToken: "loop", Namespaces: sel}); err == nil {
		t.Fatal("expected repeated page token")
	}
}

func TestClientDiffPagesRename(t *testing.T) {
	t.Parallel()
	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.RequestURI())
		if r.URL.Query().Get("content") != "" {
			http.Error(w, "diff has no content param", http.StatusBadRequest)
			return
		}
		if !strings.Contains(r.URL.Path, "/trees/main@aaa/diff/main@bbb") {
			http.NotFound(w, r)
			return
		}
		switch r.URL.Query().Get("page-token") {
		case "":
			if r.URL.Query().Get("max-records") != "2" {
				http.Error(w, "max", http.StatusBadRequest)
				return
			}
			w.Write([]byte(`{
				"diffs": [
					{"key":{"elements":["db","events"]},"from":{"type":"ICEBERG_TABLE","id":"id-1","metadataLocation":"s3://old","snapshotId":1,"schemaId":0,"specId":0,"sortOrderId":0},"to":null},
					{"key":{"elements":["db","legacy"]},"from":{"type":"DELTA_LAKE_TABLE","id":"d"},"to":null}
				],
				"hasMore": true,
				"token": "p2"
			}`))
		case "p2":
			w.Write([]byte(`{
				"diffs": [
					{"key":{"elements":["db","events2"]},"from":null,"to":{"type":"ICEBERG_TABLE","id":"id-1","metadataLocation":"s3://old","snapshotId":1,"schemaId":0,"specId":0,"sortOrderId":0}}
				],
				"hasMore": false
			}`))
		default:
			http.Error(w, "token", http.StatusBadRequest)
		}
	}))
	t.Cleanup(srv.Close)
	client := mustClient(t, srv, Config{PageSize: 2, Refs: []catalog.Ref{{Name: "main"}}})
	ref := catalog.Ref{Name: "main", Kind: catalog.RefBranch}
	same, err := client.Diff(context.Background(), ref, catalog.Revision{Token: "aaa"}, catalog.Revision{Token: "aaa"})
	if err != nil || len(same) != 0 || len(paths) != 0 {
		t.Fatalf("same %+v err %v paths %v", same, err, paths)
	}
	changes, err := client.Diff(context.Background(), ref, catalog.Revision{Token: "aaa"}, catalog.Revision{Token: "bbb"})
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 || changes[0].Kind != catalog.ChangeUpsert || changes[0].Pointer.Name != "events2" {
		t.Fatalf("changes %+v", changes)
	}
	if len(paths) != 2 || !strings.Contains(paths[0], "/trees/main@aaa/diff/main@bbb") {
		t.Fatalf("paths %v", paths)
	}
	if _, err := client.Diff(context.Background(), ref, catalog.Revision{}, catalog.Revision{Token: "bbb"}); err == nil {
		t.Fatal("empty token")
	}
}

func TestClientHistoryStopsAtKnownHash(t *testing.T) {
	t.Parallel()
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/api/v2/trees/main/history" {
			http.NotFound(w, r)
			return
		}
		if r.URL.Query().Get("max-records") != "2" {
			http.Error(w, "max", http.StatusBadRequest)
			return
		}
		switch r.URL.Query().Get("page-token") {
		case "":
			w.Write([]byte(`{
				"logEntries": [
					{"commitMeta":{"hash":"ccc","commitTime":"2024-05-01T00:00:00Z","message":"head"}},
					{"commitMeta":{"hash":"bbb","commitTime":"2024-04-01T00:00:00Z","message":"mid"}}
				],
				"hasMore": true,
				"token": "p2"
			}`))
		case "p2":
			w.Write([]byte(`{
				"logEntries": [
					{"commitMeta":{"hash":"aaa","commitTime":"2024-03-01T00:00:00.123Z","message":"known"}}
				],
				"hasMore": true,
				"token": "p3"
			}`))
		default:
			http.Error(w, "should stop", http.StatusBadRequest)
		}
	}))
	t.Cleanup(srv.Close)
	client := mustClient(t, srv, Config{HistoryMaxRecords: 2, Refs: []catalog.Ref{{Name: "main"}}})
	ref := catalog.Ref{Name: "main", Kind: catalog.RefBranch}
	got, err := client.History(context.Background(), ref, "aaa")
	if err != nil {
		t.Fatal(err)
	}
	if !got.KnownReached || got.NewCommits != 2 || got.Head.Token != "ccc" || !got.Head.CommitTime.Equal(time.Date(2024, 5, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("history %+v", got)
	}
	if calls != 2 {
		t.Fatalf("calls %d", calls)
	}

	headOnly := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("page-token") != "" {
			http.Error(w, "followed token", http.StatusBadRequest)
			return
		}
		w.Write([]byte(`{"logEntries":[{"commitMeta":{"hash":"ccc","commitTime":"2024-05-01T00:00:00Z"}},{"commitMeta":{"hash":"bbb","commitTime":"2024-04-01T00:00:00Z"}}],"hasMore":true,"token":"more"}`))
	}))
	t.Cleanup(headOnly.Close)
	headClient := mustClient(t, headOnly, Config{Refs: []catalog.Ref{{Name: "main"}}})
	got, err = headClient.History(context.Background(), ref, "")
	if err != nil {
		t.Fatal(err)
	}
	if got.KnownReached || got.NewCommits != 0 || got.Head.Token != "ccc" {
		t.Fatalf("empty known %+v", got)
	}

	missing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"logEntries":[{"commitMeta":{"hash":"ccc","commitTime":"2024-05-01T00:00:00Z"}}],"hasMore":false}`))
	}))
	t.Cleanup(missing.Close)
	missingClient := mustClient(t, missing, Config{Refs: []catalog.Ref{{Name: "main"}}})
	got, err = missingClient.History(context.Background(), ref, "nope")
	if err != nil {
		t.Fatal(err)
	}
	if got.KnownReached || got.NewCommits != 1 || got.Head.Token != "ccc" {
		t.Fatalf("missing %+v", got)
	}
}

func TestClientErrorsRedactTokenAndStripQuery(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "token=super-secret failed", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)
	client := mustClient(t, srv, Config{Token: "super-secret", Refs: []catalog.Ref{{Name: "main"}}})
	err := client.Probe(context.Background())
	var status *statusError
	if !errors.As(err, &status) || status.status != http.StatusInternalServerError {
		t.Fatalf("err %v", err)
	}
	if strings.Contains(err.Error(), "super-secret") || !strings.Contains(err.Error(), "[redacted]") {
		t.Fatalf("error %q", err.Error())
	}
	if strings.Contains(err.Error(), "?") {
		t.Fatalf("query leaked %q", err.Error())
	}

	dead := mustClient(t, srv, Config{Token: "super-secret", Refs: []catalog.Ref{{Name: "main"}}})
	dead.baseURL = "http://127.0.0.1:1/api/v2"
	_, err = dead.ListTables(context.Background(), catalog.Ref{Name: "main"}, catalog.ListOptions{})
	if err == nil || strings.Contains(err.Error(), "super-secret") || strings.Contains(err.Error(), "content=true") || strings.Contains(err.Error(), "?") {
		t.Fatalf("transport error %q", err)
	}
}

func TestClientEmptyHashAndCanceled(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"type":"BRANCH","name":"main","hash":""}`))
	}))
	t.Cleanup(srv.Close)
	client := mustClient(t, srv, Config{Refs: []catalog.Ref{{Name: "main"}}})
	if _, err := client.Revision(context.Background(), catalog.Ref{Name: "main"}); err == nil {
		t.Fatal("empty hash")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.Revision(ctx, catalog.Ref{Name: "main"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("err %v", err)
	}
	if _, err := client.ListTables(ctx, catalog.Ref{Name: "main"}, catalog.ListOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("list err %v", err)
	}
}

func TestRefreshHashShortCircuitThenDiff(t *testing.T) {
	t.Parallel()
	hash := "aaa"
	var mu sync.Mutex
	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		current := hash
		mu.Unlock()
		switch r.URL.Path {
		case "/api/v2/trees/main":
			io.WriteString(w, `{"type":"BRANCH","name":"main","hash":"`+current+`"}`)
		case "/api/v2/trees/feature":
			io.WriteString(w, `{"type":"BRANCH","name":"feature","hash":"feat"}`)
		case "/api/v2/trees/main/entries":
			if r.URL.Query().Get("content") != "true" {
				http.Error(w, "content", http.StatusBadRequest)
				return
			}
			w.Write([]byte(`{
				"entries": [
					{"type":"ICEBERG_TABLE","name":{"elements":["db","events"]},"contentId":"cid","content":{"type":"ICEBERG_TABLE","id":"cid","metadataLocation":"s3://wh/v1.metadata.json","snapshotId":1,"schemaId":0,"specId":0,"sortOrderId":0}},
					{"type":"ICEBERG_VIEW","name":{"elements":["db","orders_view"]},"contentId":"view","content":{"type":"ICEBERG_VIEW","id":"view","metadataLocation":"s3://wh/view.metadata.json","versionId":1,"schemaId":0}}
				],
				"hasMore": false
			}`))
		case "/api/v2/trees/feature/entries":
			w.Write([]byte(`{
				"entries": [
					{"type":"ICEBERG_TABLE","name":{"elements":["db","events"]},"contentId":"cid","content":{"type":"ICEBERG_TABLE","id":"cid","metadataLocation":"s3://wh/v2.metadata.json","snapshotId":2,"schemaId":0,"specId":0,"sortOrderId":0}}
				],
				"hasMore": false
			}`))
		case "/api/v2/trees/main@aaa/diff/main@bbb":
			w.Write([]byte(`{
				"diffs": [
					{"key":{"elements":["db","events"]},"from":{"type":"ICEBERG_TABLE","id":"cid","metadataLocation":"s3://wh/v1.metadata.json","snapshotId":1,"schemaId":0,"specId":0,"sortOrderId":0},"to":{"type":"ICEBERG_TABLE","id":"cid","metadataLocation":"s3://wh/v2.metadata.json","snapshotId":2,"schemaId":0,"specId":0,"sortOrderId":0}}
				],
				"hasMore": false
			}`))
		default:
			if strings.Contains(r.URL.Path, "/contents") || strings.Contains(r.URL.Path, "/history") {
				http.Error(w, "unexpected "+r.URL.Path, http.StatusBadRequest)
				return
			}
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	client := mustClient(t, srv, Config{
		Refs:    []catalog.Ref{{Name: "feature"}, {Name: "main"}},
		BaseRef: "main",
	})
	state := catalog.NewState()
	now := time.Unix(1_700, 0).UTC()
	ref := catalog.Ref{Name: "main", Kind: catalog.RefBranch}

	first, err := catalog.Refresh(context.Background(), client, ref, state, catalog.Policy{Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if first.Mode != catalog.ModeList || len(first.Tables) != 1 || len(first.Views) != 1 {
		t.Fatalf("first mode %s tables %d views %d", first.Mode, len(first.Tables), len(first.Views))
	}
	if len(first.FetchMetadata) != 1 || first.FetchMetadata[0].Type == catalog.ContentView {
		t.Fatalf("fetch %+v", first.FetchMetadata)
	}

	second, err := catalog.Refresh(context.Background(), client, ref, state, catalog.Policy{Now: now.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if second.Mode != catalog.ModeCache || second.Revision.Token != "aaa" || second.Reused != 2 {
		t.Fatalf("second mode %s token %s reused %d", second.Mode, second.Revision.Token, second.Reused)
	}

	mu.Lock()
	hash = "bbb"
	mu.Unlock()
	third, err := catalog.Refresh(context.Background(), client, ref, state, catalog.Policy{Now: now.Add(2 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if third.Mode != catalog.ModeDiff || len(third.Changed) != 1 || third.Changed[0].MetadataLocation != "s3://wh/v2.metadata.json" {
		t.Fatalf("third mode %s changed %+v", third.Mode, third.Changed)
	}
	if third.Reused != 1 || len(third.Views) != 1 {
		t.Fatalf("reused %d views %d", third.Reused, len(third.Views))
	}

	fourth, err := catalog.Refresh(context.Background(), client, ref, state, catalog.Policy{Now: now.Add(3 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if fourth.Mode != catalog.ModeCache {
		t.Fatalf("fourth %s", fourth.Mode)
	}

	mu.Lock()
	seen := append([]string(nil), paths...)
	mu.Unlock()
	var entries, diffs int
	for _, path := range seen {
		switch path {
		case "/api/v2/trees/main/entries":
			entries++
		case "/api/v2/trees/main@aaa/diff/main@bbb":
			diffs++
		}
	}
	if entries != 1 || diffs != 1 {
		t.Fatalf("entries %d diffs %d paths %v", entries, diffs, seen)
	}

	results, err := catalog.RefreshAll(context.Background(), client, catalog.NewState(), catalog.Policy{Now: now.Add(4 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || results[0].Ref.Name != "main" || results[1].Ref.Name != "feature" || !results[1].BaseCompared {
		t.Fatalf("results %+v", results)
	}
	if len(results[1].DivergedFromBase) != 1 || results[1].DivergedFromBase[0].Name != "events" {
		t.Fatalf("diverged %+v", results[1].DivergedFromBase)
	}
}

func TestClientRedirectDropsAuthorization(t *testing.T) {
	t.Parallel()
	var otherAuth string
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		otherAuth = r.Header.Get("Authorization")
		w.Write([]byte(`{"specVersion":"2.2.0"}`))
	}))
	t.Cleanup(other.Close)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL+"/api/v2/config", http.StatusFound)
	}))
	t.Cleanup(origin.Close)
	client := mustClient(t, origin, Config{Token: "secret", Refs: []catalog.Ref{{Name: "main"}}})
	if err := client.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	if otherAuth != "" {
		t.Fatalf("authorization forwarded: %q", otherAuth)
	}
}

type recorded struct {
	path  string
	query string
}

func record(r *http.Request) recorded {
	return recorded{path: r.URL.Path, query: r.URL.RawQuery}
}

func pageTokenOf(r *http.Request) string {
	return r.URL.Query().Get("page-token")
}

func TestRevisionWrappedReference(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v2/trees/main" {
			http.NotFound(w, r)
			return
		}
		w.Write([]byte(`{"reference":{"type":"BRANCH","name":"main","hash":"wrapped"}}`))
	}))
	defer srv.Close()
	client := mustClient(t, srv, Config{Refs: []catalog.Ref{{Name: "main", Kind: catalog.RefBranch}}})
	rev, err := client.Revision(context.Background(), catalog.Ref{Name: "main", Kind: catalog.RefBranch})
	if err != nil {
		t.Fatal(err)
	}
	if rev.Token != "wrapped" {
		t.Fatalf("token %q", rev.Token)
	}
}

func mustClient(t *testing.T, srv *httptest.Server, cfg Config) *Client {
	t.Helper()
	cfg.Name = "warehouse"
	cfg.BaseURL = srv.URL
	client, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
