package catalog

import (
	"context"
	"encoding/json"
	"time"
)

// Catalog is the required port. Every Iceberg catalog can probe, list refs,
// and page through table pointers. Name is the Prometheus label, not the
// adapter type: two Nessie catalogs on one exporter stay distinct.
type Catalog interface {
	Name() string
	Type() string
	Probe(ctx context.Context) error
	ListRefs(ctx context.Context) ([]Ref, error)
	ListTables(ctx context.Context, ref Ref, opts ListOptions) (TablePage, error)
}

// Revisioned is a cheap generation token for a ref.
// Nessie uses the commit hash. When the token matches the cached one and a
// full reconcile is not due, Refresh does not call ListTables.
type Revisioned interface {
	Revision(ctx context.Context, ref Ref) (Revision, error)
}

// Diffable is an incremental change set between two tokens of one ref.
// Nessie uses GET /trees/{from}/diff/{to}. A full ListTables remains the cold
// start and the periodic reconcile. Upserts must carry the new pointer;
// otherwise Refresh falls back to a full list.
type Diffable interface {
	Diff(ctx context.Context, ref Ref, from, to Revision) ([]Change, error)
}

// MultiRef catalogs expose several refs and a base ref. Refresh reports how
// many tables on the other ref have a different snapshot id than the base.
type MultiRef interface {
	BaseRef() (Ref, bool)
}

// CommitLog is one walk of a ref's commit log.
// NewCommits counts tokens strictly newer than the token passed to History.
// It is a gauge of that walk, not a counter. KnownReached is false when the
// previous token was not in the walked log; NewCommits is then only how many
// commits were read. An empty known token reports HEAD and zero new commits.
type CommitLog struct {
	Head         Revision
	NewCommits   int
	KnownReached bool
}

// Historian pages a ref's commit log. Nessie uses
// GET /trees/{ref}/history. Catalogs without a commit log do not implement it,
// and the exporter then omits iceberg_catalog_commits_since_scrape.
type Historian interface {
	History(ctx context.Context, ref Ref, knownToken string) (CommitLog, error)
}

// Ref is a branch, a tag, or the single synthetic ref used by catalogs that
// have no branches. REST, Glue, Hive, and Hadoop use a synthetic ref, default
// name "main", so metric labels stay aligned with Nessie.
type Ref struct {
	Name string
	Kind RefKind
}

// RefKind classifies a ref. The empty kind is preserved and is part of the
// cache key, so callers must pass the same kind they listed.
type RefKind string

const (
	RefBranch    RefKind = "branch"
	RefTag       RefKind = "tag"
	RefSynthetic RefKind = "synthetic"
)

// Revision is the generation token of one ref.
// Token equality ignores CommitTime. An empty token never matches.
type Revision struct {
	Token      string
	CommitTime time.Time
}

// SameToken reports whether both revisions carry the same non-empty token.
func (r Revision) SameToken(other Revision) bool {
	return r.Token != "" && r.Token == other.Token
}

// ListOptions is one page request. Adapters should apply Namespaces themselves;
// ListAllTables applies the same selector again so a forgetful adapter cannot
// leak excluded namespaces into the cache.
type ListOptions struct {
	PageToken  string
	PageSize   int
	Namespaces NamespaceSelector
}

// TablePage is one page of pointers. An empty NextPageToken ends the scan.
type TablePage struct {
	Tables        []TablePointer
	NextPageToken string
}

// ContentType separates tables from views. Views are listed and cached, and
// they never request a metadata fetch. The empty type is treated as a table.
type ContentType string

const (
	ContentTable ContentType = "table"
	ContentView  ContentType = "view"
)

// TablePointer is a catalog pointer, not table statistics.
// Metadata is set when the adapter already has metadata JSON (Iceberg REST
// loadTable often does). Nessie's native API leaves it empty and the exporter
// reads the file from the warehouse once.
//
// Metadata bytes returned from the cache must not be mutated.
type TablePointer struct {
	ContentID        string
	Namespace        string
	Name             string
	MetadataLocation string
	SnapshotID       *int64
	Metadata         json.RawMessage
	Type             ContentType
}

// ChangeKind is one diff entry. The zero value is not a valid kind.
type ChangeKind int

const (
	ChangeUpsert ChangeKind = iota + 1
	ChangeDelete
)

// Change is one key in a Diffable result.
// Upsert requires Pointer.Name. Delete requires ContentID or Name.
type Change struct {
	Kind    ChangeKind
	Pointer TablePointer
}
