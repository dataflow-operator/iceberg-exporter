package nessie

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"iceberg-exporter/internal/catalog"
)

const (
	contentIcebergTable = "ICEBERG_TABLE"
	contentIcebergView  = "ICEBERG_VIEW"
)

type referenceJSON struct {
	Type string `json:"type"`
	Name string `json:"name"`
	Hash string `json:"hash"`
}

// referenceEnvelope accepts the flat reference used by older Nessie responses
// and the wrapped document current servers return from GET /trees/{ref}.
type referenceEnvelope struct {
	Reference referenceJSON `json:"reference"`
	referenceJSON
}

func (e referenceEnvelope) ref() referenceJSON {
	if e.Reference.Hash != "" {
		return e.Reference
	}
	return e.referenceJSON
}

type contentKeyJSON struct {
	Elements []string `json:"elements"`
}

type entriesResponse struct {
	Entries []entryJSON `json:"entries"`
	Token   string      `json:"token"`
	HasMore bool        `json:"hasMore"`
}

type entryJSON struct {
	Type      string          `json:"type"`
	Name      contentKeyJSON  `json:"name"`
	ContentID string          `json:"contentId"`
	Content   json.RawMessage `json:"content"`
}

type diffResponse struct {
	Diffs   []diffEntryJSON `json:"diffs"`
	Token   string          `json:"token"`
	HasMore bool            `json:"hasMore"`
}

type diffEntryJSON struct {
	Key  contentKeyJSON  `json:"key"`
	From json.RawMessage `json:"from"`
	To   json.RawMessage `json:"to"`
}

type logResponse struct {
	LogEntries []logEntryJSON `json:"logEntries"`
	Token      string         `json:"token"`
	HasMore    bool           `json:"hasMore"`
}

type logEntryJSON struct {
	CommitMeta commitMetaJSON `json:"commitMeta"`
}

type commitMetaJSON struct {
	Hash       string `json:"hash"`
	CommitTime string `json:"commitTime"`
}

type icebergBody struct {
	Type             string `json:"type"`
	ID               string `json:"id"`
	MetadataLocation string `json:"metadataLocation"`
	SnapshotID       *int64 `json:"snapshotId"`
	VersionID        *int64 `json:"versionId"`
}

func unmarshalJSON(body []byte, dest any) error {
	dec := json.NewDecoder(bytes.NewReader(body))
	if err := dec.Decode(dest); err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	return nil
}

func changesFrom(diffs []diffEntryJSON) ([]catalog.Change, error) {
	type side struct {
		ok bool
		p  catalog.TablePointer
	}
	items := make([]struct{ from, to side }, 0, len(diffs))
	upsertIDs := make(map[string]struct{})
	for _, diff := range diffs {
		var item struct{ from, to side }
		from, ok, err := decodeContent(diff.Key.Elements, "", "", diff.From)
		if err != nil {
			return nil, err
		}
		if ok {
			item.from = side{ok: true, p: from}
		}
		to, ok, err := decodeContent(diff.Key.Elements, "", "", diff.To)
		if err != nil {
			return nil, err
		}
		if ok {
			item.to = side{ok: true, p: to}
			if to.ContentID != "" {
				upsertIDs[to.ContentID] = struct{}{}
			}
		}
		items = append(items, item)
	}

	var out []catalog.Change
	for _, item := range items {
		if item.from.ok {
			id := item.from.p.ContentID
			if _, moved := upsertIDs[id]; id == "" || !moved {
				out = append(out, catalog.Change{Kind: catalog.ChangeDelete, Pointer: item.from.p})
			}
		}
		if item.to.ok {
			out = append(out, catalog.Change{Kind: catalog.ChangeUpsert, Pointer: item.to.p})
		}
	}
	return out, nil
}

func decodeContent(elements []string, fallbackType, fallbackID string, raw json.RawMessage) (catalog.TablePointer, bool, error) {
	if absent(raw) && fallbackType == "" {
		return catalog.TablePointer{}, false, nil
	}
	var body icebergBody
	if !absent(raw) {
		if err := json.Unmarshal(raw, &body); err != nil {
			return catalog.TablePointer{}, false, fmt.Errorf("nessie: content: %w", err)
		}
	}
	typ := body.Type
	if typ == "" {
		typ = fallbackType
	}
	var contentType catalog.ContentType
	var snapshot *int64
	switch typ {
	case contentIcebergTable:
		contentType = catalog.ContentTable
		snapshot = cloneID(body.SnapshotID)
	case contentIcebergView:
		contentType = catalog.ContentView
		snapshot = cloneID(body.VersionID)
	default:
		return catalog.TablePointer{}, false, nil
	}
	if len(elements) == 0 {
		return catalog.TablePointer{}, false, errors.New("nessie: content key is empty")
	}
	for _, element := range elements {
		if element == "" {
			return catalog.TablePointer{}, false, errors.New("nessie: content key has an empty element")
		}
	}
	id := body.ID
	if id == "" {
		id = fallbackID
	}
	return catalog.TablePointer{
		ContentID:        id,
		Namespace:        catalog.JoinNamespace(elements[:len(elements)-1]...),
		Name:             elements[len(elements)-1],
		MetadataLocation: body.MetadataLocation,
		SnapshotID:       snapshot,
		Type:             contentType,
	}, true, nil
}

func absent(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null"))
}

func cloneID(id *int64) *int64 {
	if id == nil {
		return nil
	}
	value := *id
	return &value
}
