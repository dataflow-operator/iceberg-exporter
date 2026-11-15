package catalog

import (
	"bytes"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"
)

// State is the per-ref pointer cache. Published generations are immutable.
// Snapshot does not wait for a list or a diff that Refresh is running.
// Refresh and BindMetadata for the same ref run one at a time.
//
// BindMetadata must not be called from a Catalog method: Refresh holds the
// ref lock across adapter calls.
type State struct {
	mu   sync.Mutex
	refs map[string]*refSlot
}

type refSlot struct {
	refresh sync.Mutex
	cache   atomic.Pointer[refCache]
}

type refCache struct {
	revision  Revision
	checkedAt time.Time
	listedAt  time.Time
	tables    map[string]TablePointer
	names     map[string]string
}

// RefSnapshot is a copy of one ref's published cache.
type RefSnapshot struct {
	Ref       Ref
	Revision  Revision
	CheckedAt time.Time
	ListedAt  time.Time
	Tables    []TablePointer
	Views     []TablePointer
}

// NewState returns an empty cache.
func NewState() *State {
	return &State{refs: make(map[string]*refSlot)}
}

func (s *State) slot(ref Ref) *refSlot {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := refKey(ref)
	sl := s.refs[k]
	if sl == nil {
		sl = &refSlot{}
		s.refs[k] = sl
	}
	return sl
}

func (s *State) existing(ref Ref) *refSlot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.refs[refKey(ref)]
}

// Snapshot copies the published pointers for ref.
// The bool is false when the ref has never been loaded.
func (s *State) Snapshot(ref Ref) (RefSnapshot, bool) {
	if s == nil {
		return RefSnapshot{}, false
	}
	sl := s.existing(ref)
	if sl == nil {
		return RefSnapshot{}, false
	}
	cache := sl.cache.Load()
	if cache == nil {
		return RefSnapshot{}, false
	}
	return cache.snapshot(ref), true
}

// BindMetadata stores metadata JSON fetched from the warehouse and, when the
// cached pointer still has no catalog content id, rekeys it to table-uuid.
// A catalog content id is left unchanged. ok is false when the table is not
// in the cache. Empty metadata is ignored.
func (s *State) BindMetadata(ref Ref, namespace, name string, metadata json.RawMessage) (TablePointer, bool) {
	if s == nil || ref.Name == "" || name == "" || len(metadata) == 0 {
		return TablePointer{}, false
	}
	sl := s.slot(ref)
	sl.refresh.Lock()
	defer sl.refresh.Unlock()

	old := sl.cache.Load()
	if old == nil {
		return TablePointer{}, false
	}
	key, cur, ok := old.lookup(TablePointer{Namespace: namespace, Name: name})
	if !ok {
		return TablePointer{}, false
	}
	updated := cur
	id := updated.ContentID
	if id == "" {
		id = tableUUID(metadata)
	}
	if id == cur.ContentID && bytes.Equal(cur.Metadata, metadata) {
		return exportPointer(cur), true
	}
	next := old.clone()
	delete(next.tables, key)
	delete(next.names, nameKey(cur.Namespace, cur.Name))
	updated.ContentID = id
	copied := make(json.RawMessage, len(metadata))
	copy(copied, metadata)
	updated.Metadata = copied
	newKey := pointerKey(updated)
	next.tables[newKey] = updated
	next.names[nameKey(updated.Namespace, updated.Name)] = newKey
	sl.cache.Store(next)
	return exportPointer(updated), true
}

func (c *refCache) snapshot(ref Ref) RefSnapshot {
	out := RefSnapshot{
		Ref:       ref,
		Revision:  c.revision,
		CheckedAt: c.checkedAt,
		ListedAt:  c.listedAt,
	}
	for _, p := range c.tables {
		exported := exportPointer(p)
		if p.Type == ContentView {
			out.Views = append(out.Views, exported)
			continue
		}
		out.Tables = append(out.Tables, exported)
	}
	sortPointers(out.Tables)
	sortPointers(out.Views)
	return out
}

func (c *refCache) clone() *refCache {
	next := *c
	next.tables = make(map[string]TablePointer, len(c.tables))
	for k, v := range c.tables {
		next.tables[k] = v
	}
	next.names = make(map[string]string, len(c.names))
	for k, v := range c.names {
		next.names[k] = v
	}
	return &next
}

// cloneHeader copies revision timestamps and shares the maps.
// The shared maps must stay immutable, which they do: a later edit clones first.
func (c *refCache) cloneHeader() *refCache {
	next := *c
	return &next
}

func (c *refCache) lookup(p TablePointer) (string, TablePointer, bool) {
	if c == nil {
		return "", TablePointer{}, false
	}
	if p.ContentID != "" {
		k := pointerKey(TablePointer{ContentID: p.ContentID})
		if cur, ok := c.tables[k]; ok {
			return k, cur, true
		}
	}
	if p.Name == "" {
		return "", TablePointer{}, false
	}
	k, ok := c.names[nameKey(p.Namespace, p.Name)]
	if !ok {
		return "", TablePointer{}, false
	}
	cur, ok := c.tables[k]
	if !ok {
		return "", TablePointer{}, false
	}
	if p.ContentID != "" && cur.ContentID != "" && cur.ContentID != p.ContentID {
		return "", TablePointer{}, false
	}
	return k, cur, true
}

func refKey(ref Ref) string {
	return string(ref.Kind) + "\x00" + ref.Name
}
