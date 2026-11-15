package catalog

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// RefreshMode is how a Refresh produced its result.
type RefreshMode int

const (
	// ModeCache means the published pointers were already current.
	// For a Revisioned catalog this follows a matching token. For every other
	// catalog it follows ListInterval.
	ModeCache RefreshMode = iota
	// ModeDiff means a Diffable adapter applied an incremental change set.
	ModeDiff
	// ModeList means pointers were listed and compared by metadataLocation.
	ModeList
)

func (m RefreshMode) String() string {
	switch m {
	case ModeCache:
		return "cache"
	case ModeDiff:
		return "diff"
	case ModeList:
		return "list"
	default:
		return fmt.Sprintf("mode(%d)", int(m))
	}
}

// Policy controls how often a ref is listed.
//
// Revisioned catalogs check the token on every Refresh. ListInterval is not
// used for them. ReconcileInterval forces a full list even when the token is
// unchanged or a diff is available, including on Nessie. Zero disables that timer.
//
// Catalogs without Revisioned serve the cached list until ListInterval elapses,
// then list and compare metadataLocation. Zero lists on every Refresh.
type Policy struct {
	Now               time.Time
	ListInterval      time.Duration
	ReconcileInterval time.Duration
	Namespaces        NamespaceSelector
	PageSize          int
	ForceFull         bool
}

// Result is the published view of one ref after Refresh.
// Tables and Views are the full set to expose. Changed are new pointers and
// pointers whose metadataLocation or snapshot id moved. FetchMetadata is the
// subset of Changed that still needs a warehouse read. Reused counts pointers
// whose previous table metrics stay valid.
type Result struct {
	Ref       Ref
	Mode      RefreshMode
	Revision  Revision
	CheckedAt time.Time
	ListedAt  time.Time

	Tables  []TablePointer
	Views   []TablePointer
	Changed []TablePointer
	// FetchMetadata needs a metadata JSON read. Embedded JSON and views are omitted.
	FetchMetadata []TablePointer
	Removed       []TablePointer
	Reused        int

	// BaseCompared is set when this ref was compared with a MultiRef base that
	// is already cached. DivergedFromBase is empty when every table matches.
	BaseCompared     bool
	DivergedFromBase []TablePointer
}

var errIncompleteDiff = errors.New("catalog: diff change is missing a table pointer")

// Refresh loads ref according to the capabilities cat actually implements.
func Refresh(ctx context.Context, cat Catalog, ref Ref, state *State, policy Policy) (Result, error) {
	if cat == nil || state == nil {
		return Result{}, errors.New("catalog: nil catalog or state")
	}
	if ref.Name == "" {
		return Result{}, errors.New("catalog: empty ref name")
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if policy.Now.IsZero() {
		policy.Now = time.Now()
	}

	sl := state.slot(ref)
	sl.refresh.Lock()
	defer sl.refresh.Unlock()

	planned, err := planRefresh(ctx, cat, ref, sl.cache.Load(), policy)
	if err != nil {
		return Result{}, err
	}
	if planned.store {
		sl.cache.Store(planned.next)
	}
	attachDivergence(cat, ref, state, &planned.result)
	return planned.result, nil
}

// RefreshAll lists refs, refreshes each one, and orders the base ref first
// when the catalog implements MultiRef. A later ref's error leaves earlier
// refs published and returns the results collected so far.
func RefreshAll(ctx context.Context, cat Catalog, state *State, policy Policy) ([]Result, error) {
	if cat == nil || state == nil {
		return nil, errors.New("catalog: nil catalog or state")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if policy.Now.IsZero() {
		policy.Now = time.Now()
	}
	refs, err := cat.ListRefs(ctx)
	if err != nil {
		return nil, fmt.Errorf("catalog: list refs: %w", err)
	}
	if multi, ok := cat.(MultiRef); ok {
		if base, ok := multi.BaseRef(); ok {
			refs = baseFirst(refs, base)
		}
	}
	seen := make(map[string]struct{}, len(refs))
	results := make([]Result, 0, len(refs))
	for _, ref := range refs {
		k := refKey(ref)
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		res, err := Refresh(ctx, cat, ref, state, policy)
		if err != nil {
			return results, err
		}
		results = append(results, res)
	}
	return results, nil
}

type planned struct {
	result Result
	next   *refCache
	store  bool
}

func planRefresh(ctx context.Context, cat Catalog, ref Ref, old *refCache, policy Policy) (planned, error) {
	var fetched *Revision
	if revCat, ok := cat.(Revisioned); ok {
		rev, err := revCat.Revision(ctx, ref)
		if err != nil {
			return planned{}, fmt.Errorf("catalog: revision ref %q: %w", ref.Name, err)
		}
		fetched = &rev
		if !policy.ForceFull && rev.Token != "" {
			same := old != nil && old.revision.SameToken(rev)
			due := reconcileDue(old, policy)
			if same && !due {
				next := touchRevision(old, rev, policy.Now)
				return planned{
					result: makeResult(ref, ModeCache, next, nil, nil),
					next:   next,
					store:  true,
				}, nil
			}
			if !due && old != nil && old.revision.Token != "" {
				if diffed, ok, err := tryDiff(ctx, cat, ref, old, rev, policy); err != nil {
					return planned{}, err
				} else if ok {
					return diffed, nil
				}
			}
		}
	} else if !policy.ForceFull && listCacheFresh(old, policy) {
		return planned{result: makeResult(ref, ModeCache, old, nil, nil)}, nil
	}

	listed, err := ListAllTables(ctx, cat, ref, ListOptions{
		PageSize:   policy.PageSize,
		Namespaces: policy.Namespaces,
	})
	if err != nil {
		return planned{}, err
	}
	next, changed, removed := rebuild(old, listed, revisionToStore(fetched, old), policy.Now)
	return planned{
		result: makeResult(ref, ModeList, next, changed, removed),
		next:   next,
		store:  true,
	}, nil
}

func tryDiff(ctx context.Context, cat Catalog, ref Ref, old *refCache, rev Revision, policy Policy) (planned, bool, error) {
	diffCat, ok := cat.(Diffable)
	if !ok {
		return planned{}, false, nil
	}
	changes, err := diffCat.Diff(ctx, ref, old.revision, rev)
	if err != nil {
		return planned{}, false, fmt.Errorf("catalog: diff ref %q: %w", ref.Name, err)
	}
	if err := classifyChanges(changes); err != nil {
		if errors.Is(err, errIncompleteDiff) {
			return planned{}, false, nil
		}
		return planned{}, false, err
	}
	next, changed, removed := applyDiff(old, changes, rev, policy.Now, policy.Namespaces)
	return planned{
		result: makeResult(ref, ModeDiff, next, changed, removed),
		next:   next,
		store:  true,
	}, true, nil
}

func classifyChanges(changes []Change) error {
	for _, ch := range changes {
		switch ch.Kind {
		case ChangeUpsert:
			if ch.Pointer.Name == "" {
				return errIncompleteDiff
			}
		case ChangeDelete:
			if ch.Pointer.ContentID == "" && ch.Pointer.Name == "" {
				return errIncompleteDiff
			}
		default:
			return fmt.Errorf("catalog: unknown change kind %d", int(ch.Kind))
		}
	}
	return nil
}

func reconcileDue(old *refCache, policy Policy) bool {
	if policy.ReconcileInterval <= 0 || old == nil || old.listedAt.IsZero() {
		return false
	}
	return !policy.Now.Before(old.listedAt.Add(policy.ReconcileInterval))
}

func listCacheFresh(old *refCache, policy Policy) bool {
	if old == nil || policy.ListInterval <= 0 || old.listedAt.IsZero() {
		return false
	}
	return policy.Now.Before(old.listedAt.Add(policy.ListInterval))
}

func touchRevision(old *refCache, rev Revision, now time.Time) *refCache {
	next := old.cloneHeader()
	if rev.CommitTime.IsZero() {
		rev.CommitTime = old.revision.CommitTime
	}
	next.revision = rev
	next.checkedAt = now
	return next
}

func revisionToStore(fetched *Revision, old *refCache) Revision {
	if fetched == nil {
		if old != nil {
			return old.revision
		}
		return Revision{}
	}
	rev := *fetched
	if rev.CommitTime.IsZero() && old != nil && old.revision.SameToken(rev) {
		rev.CommitTime = old.revision.CommitTime
	}
	return rev
}

func rebuild(old *refCache, listed []TablePointer, rev Revision, now time.Time) (*refCache, []TablePointer, []TablePointer) {
	next := &refCache{
		revision:  rev,
		checkedAt: now,
		listedAt:  now,
		tables:    make(map[string]TablePointer),
		names:     make(map[string]string),
	}
	consumed := make(map[string]struct{})
	changedByKey := make(map[string]TablePointer)
	var removed []TablePointer
	for _, raw := range listed {
		raw = clonePointer(raw)
		// Identity has to be known before lookup so a rename still matches
		// the previous content id or table-uuid.
		adoptIdentity(&raw)
		var prev *TablePointer
		if old != nil {
			if key, found, ok := old.lookup(raw); ok {
				prev = &found
				consumed[key] = struct{}{}
			}
		}
		merged, changed := classify(prev, raw)
		merged = clonePointer(merged)
		removed = append(removed, place(next, merged, changed, changedByKey)...)
	}
	if old != nil {
		for key, p := range old.tables {
			if _, ok := consumed[key]; !ok {
				removed = append(removed, p)
			}
		}
	}
	return next, pointerValues(changedByKey), removed
}

func applyDiff(old *refCache, changes []Change, rev Revision, now time.Time, sel NamespaceSelector) (*refCache, []TablePointer, []TablePointer) {
	next := old.clone()
	stored := rev
	if stored.CommitTime.IsZero() {
		stored.CommitTime = old.revision.CommitTime
	}
	next.revision = stored
	next.checkedAt = now

	changedByKey := make(map[string]TablePointer)
	var removed []TablePointer
	for _, ch := range changes {
		switch ch.Kind {
		case ChangeDelete:
			key, prev, ok := next.lookup(ch.Pointer)
			if !ok {
				continue
			}
			delete(next.tables, key)
			delete(next.names, nameKey(prev.Namespace, prev.Name))
			delete(changedByKey, key)
			removed = append(removed, prev)
		case ChangeUpsert:
			removed = append(removed, upsert(next, ch.Pointer, sel, changedByKey)...)
		}
	}
	return next, pointerValues(changedByKey), removed
}

func upsert(cache *refCache, raw TablePointer, sel NamespaceSelector, changedByKey map[string]TablePointer) []TablePointer {
	raw = clonePointer(raw)
	adoptIdentity(&raw)
	if !sel.Allows(raw.Namespace) {
		key, prev, ok := cache.lookup(raw)
		if !ok {
			return nil
		}
		delete(cache.tables, key)
		delete(cache.names, nameKey(prev.Namespace, prev.Name))
		delete(changedByKey, key)
		return []TablePointer{prev}
	}
	if raw.ContentID != "" && raw.Name != "" {
		if key, prev, ok := cache.lookup(TablePointer{Namespace: raw.Namespace, Name: raw.Name}); ok &&
			prev.ContentID != "" && prev.ContentID != raw.ContentID {
			delete(cache.tables, key)
			delete(cache.names, nameKey(prev.Namespace, prev.Name))
			delete(changedByKey, key)
			// The previous identity is a different table that gave up this name.
			// Fall through and insert the new one. The caller records the removal.
			removed := []TablePointer{prev}
			return append(removed, insertPointer(cache, raw, changedByKey)...)
		}
	}
	return insertPointer(cache, raw, changedByKey)
}

func insertPointer(cache *refCache, raw TablePointer, changedByKey map[string]TablePointer) []TablePointer {
	var prev *TablePointer
	var oldKey string
	if key, found, ok := cache.lookup(raw); ok {
		prev = &found
		oldKey = key
	}
	merged, changed := classify(prev, raw)
	merged = clonePointer(merged)
	if prev != nil && oldKey != pointerKey(merged) {
		delete(cache.tables, oldKey)
		delete(changedByKey, oldKey)
	}
	if prev != nil && (prev.Namespace != merged.Namespace || prev.Name != merged.Name) {
		delete(cache.names, nameKey(prev.Namespace, prev.Name))
	}
	return place(cache, merged, changed, changedByKey)
}

func place(cache *refCache, merged TablePointer, changed bool, changedByKey map[string]TablePointer) []TablePointer {
	k := pointerKey(merged)
	var displaced []TablePointer
	if merged.Name != "" {
		nk := nameKey(merged.Namespace, merged.Name)
		if prevKey, ok := cache.names[nk]; ok && prevKey != k {
			if prev, ok := cache.tables[prevKey]; ok {
				displaced = append(displaced, prev)
			}
			delete(cache.tables, prevKey)
			delete(changedByKey, prevKey)
		}
		cache.names[nk] = k
	}
	cache.tables[k] = merged
	if changed {
		changedByKey[k] = merged
	} else {
		delete(changedByKey, k)
	}
	return displaced
}

func pointerValues(byKey map[string]TablePointer) []TablePointer {
	if len(byKey) == 0 {
		return nil
	}
	out := make([]TablePointer, 0, len(byKey))
	for _, p := range byKey {
		out = append(out, p)
	}
	return out
}

func makeResult(ref Ref, mode RefreshMode, cache *refCache, changed, removed []TablePointer) Result {
	res := Result{Ref: ref, Mode: mode}
	if cache == nil {
		return res
	}
	res.Revision = cache.revision
	res.CheckedAt = cache.checkedAt
	res.ListedAt = cache.listedAt
	changedSet := make(map[string]struct{}, len(changed))
	for _, p := range changed {
		changedSet[pointerKey(p)] = struct{}{}
	}
	for _, p := range cache.tables {
		out := exportPointer(p)
		if p.Type == ContentView {
			res.Views = append(res.Views, out)
		} else {
			res.Tables = append(res.Tables, out)
		}
		if _, ok := changedSet[pointerKey(p)]; !ok {
			res.Reused++
		}
	}
	res.Changed = exportAll(changed)
	res.Removed = exportAll(removed)
	res.FetchMetadata = exportAll(selectFetch(changed))
	sortPointers(res.Tables)
	sortPointers(res.Views)
	sortPointers(res.Changed)
	sortPointers(res.Removed)
	sortPointers(res.FetchMetadata)
	return res
}

func selectFetch(changed []TablePointer) []TablePointer {
	var out []TablePointer
	for _, p := range changed {
		if needsFetch(p) {
			out = append(out, p)
		}
	}
	return out
}

func exportAll(in []TablePointer) []TablePointer {
	if len(in) == 0 {
		return nil
	}
	out := make([]TablePointer, len(in))
	for i, p := range in {
		out[i] = exportPointer(p)
	}
	return out
}

func attachDivergence(cat Catalog, ref Ref, state *State, res *Result) {
	multi, ok := cat.(MultiRef)
	if !ok {
		return
	}
	base, ok := multi.BaseRef()
	if !ok || refKey(base) == refKey(ref) {
		return
	}
	snap, ok := state.Snapshot(base)
	if !ok {
		return
	}
	res.BaseCompared = true
	basePtrs := append(append([]TablePointer{}, snap.Tables...), snap.Views...)
	other := append(append([]TablePointer{}, res.Tables...), res.Views...)
	res.DivergedFromBase = DivergedTables(basePtrs, other)
}

func baseFirst(refs []Ref, base Ref) []Ref {
	out := make([]Ref, 0, len(refs))
	rest := make([]Ref, 0, len(refs))
	key := refKey(base)
	for _, ref := range refs {
		if refKey(ref) == key {
			out = append(out, ref)
			continue
		}
		rest = append(rest, ref)
	}
	return append(out, rest...)
}
