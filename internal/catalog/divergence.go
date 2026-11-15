package catalog

import "slices"

// DivergedTables returns tables on other whose snapshot id differs from base,
// plus tables that exist only on other. Views are ignored. Tables that exist
// only on base are not counted: the metric belongs to the compared ref.
// A snapshot id is compared by content id first, then by namespace and name.
// The same content id matches across a rename. Two different content ids that
// share a name do not match.
func DivergedTables(base, other []TablePointer) []TablePointer {
	byID := make(map[string]TablePointer)
	byName := make(map[string]TablePointer)
	for _, p := range base {
		if p.Type == ContentView {
			continue
		}
		if p.ContentID != "" {
			byID[p.ContentID] = p
		}
		if p.Name != "" {
			byName[nameKey(p.Namespace, p.Name)] = p
		}
	}
	var diverged []TablePointer
	for _, p := range other {
		if p.Type == ContentView {
			continue
		}
		match, ok := matchBase(byID, byName, p)
		if !ok || snapshotsDiffer(match, p) {
			diverged = append(diverged, p)
		}
	}
	sortPointers(diverged)
	return diverged
}

func matchBase(byID, byName map[string]TablePointer, p TablePointer) (TablePointer, bool) {
	if p.ContentID != "" {
		if found, ok := byID[p.ContentID]; ok {
			return found, true
		}
	}
	if p.Name == "" {
		return TablePointer{}, false
	}
	found, ok := byName[nameKey(p.Namespace, p.Name)]
	if !ok {
		return TablePointer{}, false
	}
	if p.ContentID != "" && found.ContentID != "" && p.ContentID != found.ContentID {
		return TablePointer{}, false
	}
	return found, true
}

func snapshotsDiffer(a, b TablePointer) bool {
	switch {
	case a.SnapshotID == nil && b.SnapshotID == nil:
		return false
	case a.SnapshotID == nil || b.SnapshotID == nil:
		return true
	default:
		return *a.SnapshotID != *b.SnapshotID
	}
}

func sortPointers(ps []TablePointer) {
	slices.SortFunc(ps, func(a, b TablePointer) int {
		if c := cmpString(a.Namespace, b.Namespace); c != 0 {
			return c
		}
		if c := cmpString(a.Name, b.Name); c != 0 {
			return c
		}
		if c := cmpString(string(a.Type), string(b.Type)); c != 0 {
			return c
		}
		return cmpString(a.ContentID, b.ContentID)
	})
}

func cmpString(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
