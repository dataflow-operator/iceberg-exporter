package catalog

import (
	"encoding/json"
	"strings"
)

// JoinNamespace formats Iceberg namespace levels with dots, which is the form
// stored on TablePointer and matched by NamespaceSelector. A level must not
// itself contain a dot.
func JoinNamespace(levels ...string) string {
	return strings.Join(levels, ".")
}

func adoptIdentity(p *TablePointer) {
	if p == nil || p.ContentID != "" {
		return
	}
	if id := tableUUID(p.Metadata); id != "" {
		p.ContentID = id
	}
}

func tableUUID(metadata json.RawMessage) string {
	if len(metadata) == 0 {
		return ""
	}
	var doc struct {
		TableUUID string `json:"table-uuid"`
	}
	if err := json.Unmarshal(metadata, &doc); err != nil {
		return ""
	}
	return strings.TrimSpace(doc.TableUUID)
}

// generationChanged is true when Iceberg has published a new table generation.
// A new metadata file is written on every commit, so the location string is
// the check every catalog can answer. Snapshot id is secondary and is compared
// only when both pointers have one. A metadata rewrite that keeps the snapshot
// still changes the location and is a new generation.
func generationChanged(prev, next TablePointer) bool {
	if prev.MetadataLocation != next.MetadataLocation {
		return true
	}
	if prev.SnapshotID != nil && next.SnapshotID != nil && *prev.SnapshotID != *next.SnapshotID {
		return true
	}
	if prev.Type != "" && next.Type != "" && prev.Type != next.Type {
		return true
	}
	return false
}

func classify(prev *TablePointer, next TablePointer) (TablePointer, bool) {
	if prev == nil {
		adoptIdentity(&next)
		return next, true
	}
	changed := generationChanged(*prev, next)
	merged := next
	if !changed {
		if merged.SnapshotID == nil {
			merged.SnapshotID = prev.SnapshotID
		}
		if len(merged.Metadata) == 0 {
			merged.Metadata = prev.Metadata
		}
		if merged.Type == "" {
			merged.Type = prev.Type
		}
	}
	if merged.ContentID == "" {
		merged.ContentID = prev.ContentID
	}
	if merged.Type == "" {
		merged.Type = prev.Type
	}
	adoptIdentity(&merged)
	return merged, changed
}

func needsFetch(p TablePointer) bool {
	if p.Type == ContentView {
		return false
	}
	return len(p.Metadata) == 0
}

func clonePointer(p TablePointer) TablePointer {
	if p.SnapshotID != nil {
		id := *p.SnapshotID
		p.SnapshotID = &id
	}
	if len(p.Metadata) > 0 {
		copied := make(json.RawMessage, len(p.Metadata))
		copy(copied, p.Metadata)
		p.Metadata = copied
	}
	return p
}

func exportPointer(p TablePointer) TablePointer {
	if p.SnapshotID != nil {
		id := *p.SnapshotID
		p.SnapshotID = &id
	}
	return p
}

func pointerKey(p TablePointer) string {
	if p.ContentID != "" {
		return "id:" + p.ContentID
	}
	return nameKey(p.Namespace, p.Name)
}

func nameKey(namespace, name string) string {
	return namespace + "\x00" + name
}
