package metadata

import (
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
)

const (
	propRecords         = "total-records"
	propDataFiles       = "total-data-files"
	propFileSize        = "total-files-size"
	propDeleteFiles     = "total-delete-files"
	propPositionDeletes = "total-position-deletes"
	propEqualityDeletes = "total-equality-deletes"
	propAddedDataFiles  = "added-data-files"
	propAddedRecords    = "added-records"
	propAddedFileSize   = "added-files-size"
)

// Stats is the current metadata snapshot, without manifest entries.
// SnapshotTime and LastUpdated are absolute instants. Age is computed when
// the gauges are published, so a cached summary stays fresh across scrapes.
type Stats struct {
	FormatVersion      int
	HasSnapshot        bool
	SnapshotID         int64
	SnapshotTime       time.Time
	LastUpdated        time.Time
	Snapshots          int
	MetadataLogEntries int
	SnapshotLogEntries int
	Records            int64
	DataFiles          int64
	FileSizeBytes      int64
	DeleteFiles        int64
	PositionDeletes    int64
	EqualityDeletes    int64
	AddedDataFiles     int64
	AddedRecords       int64
	AddedFileSizeBytes int64
	// Operation is empty when the table has no current snapshot.
	// It is one of append, overwrite, delete, replace.
	Operation string
	// DataManifests, DeleteManifests, and PartitionSpecs come from the
	// current snapshot manifest list. They stay zero until that list is read.
	// PartitionSpecs is the number of distinct partition spec ids in the list.
	DataManifests   int
	DeleteManifests int
	PartitionSpecs  int
}

type parsed struct {
	Stats     Stats
	ContentID string
	// manifestList is the current snapshot manifest list location.
	// It is empty when the snapshot has no list file.
	manifestList string
	// embeddedPresent is true when the snapshot uses the v1 manifests array,
	// including an explicitly empty array. Those paths have no spec ids.
	embeddedPresent   bool
	embeddedManifests int
	embeddedPaths     []string
	metadataSpecs     int
}

// Parse reads metadata JSON with iceberg-go and keeps the current snapshot
// summary. A table with no current snapshot is valid: every total is zero
// and HasSnapshot is false.
func Parse(raw []byte) (parsed, error) {
	meta, err := table.ParseMetadataBytes(raw)
	if err != nil {
		return parsed{}, fmt.Errorf("metadata: parse: %w", err)
	}
	stats, err := statsFrom(meta)
	if err != nil {
		return parsed{}, err
	}
	id := ""
	if u := meta.TableUUID(); u != uuid.Nil {
		id = u.String()
	}
	out := parsed{
		Stats:         stats,
		ContentID:     id,
		metadataSpecs: len(meta.PartitionSpecs()),
	}
	if snap := meta.CurrentSnapshot(); snap != nil {
		out.manifestList = snap.ManifestList
		out.embeddedPresent = snap.ManifestLocations != nil
		out.embeddedManifests = len(snap.ManifestLocations)
		out.embeddedPaths = slices.Clone(snap.ManifestLocations)
	}
	return out, nil
}

func statsFrom(meta table.Metadata) (Stats, error) {
	stats := Stats{
		FormatVersion:      meta.Version(),
		LastUpdated:        millis(meta.LastUpdatedMillis()),
		Snapshots:          len(meta.Snapshots()),
		MetadataLogEntries: len(slices.Collect(meta.PreviousFiles())),
		SnapshotLogEntries: len(slices.Collect(meta.SnapshotLogs())),
	}
	snap := meta.CurrentSnapshot()
	if snap == nil {
		return stats, nil
	}
	if snap.Summary == nil {
		return Stats{}, fmt.Errorf("metadata: snapshot %d has no summary", snap.SnapshotID)
	}
	op, err := table.ValidOperation(string(snap.Summary.Operation))
	if err != nil {
		return Stats{}, fmt.Errorf("metadata: snapshot %d: %w", snap.SnapshotID, err)
	}
	props := snap.Summary.Properties
	records, err := summaryInt(props, propRecords)
	if err != nil {
		return Stats{}, err
	}
	dataFiles, err := summaryInt(props, propDataFiles)
	if err != nil {
		return Stats{}, err
	}
	fileSize, err := summaryInt(props, propFileSize)
	if err != nil {
		return Stats{}, err
	}
	deleteFiles, err := summaryInt(props, propDeleteFiles)
	if err != nil {
		return Stats{}, err
	}
	positionDeletes, err := summaryInt(props, propPositionDeletes)
	if err != nil {
		return Stats{}, err
	}
	equalityDeletes, err := summaryInt(props, propEqualityDeletes)
	if err != nil {
		return Stats{}, err
	}
	addedFiles, err := summaryInt(props, propAddedDataFiles)
	if err != nil {
		return Stats{}, err
	}
	addedRecords, err := summaryInt(props, propAddedRecords)
	if err != nil {
		return Stats{}, err
	}
	addedSize, err := summaryInt(props, propAddedFileSize)
	if err != nil {
		return Stats{}, err
	}
	stats.HasSnapshot = true
	stats.SnapshotID = snap.SnapshotID
	stats.SnapshotTime = millis(snap.TimestampMs)
	stats.Records = records
	stats.DataFiles = dataFiles
	stats.FileSizeBytes = fileSize
	stats.DeleteFiles = deleteFiles
	stats.PositionDeletes = positionDeletes
	stats.EqualityDeletes = equalityDeletes
	stats.AddedDataFiles = addedFiles
	stats.AddedRecords = addedRecords
	stats.AddedFileSizeBytes = addedSize
	stats.Operation = string(op)
	return stats, nil
}

func summaryInt(props iceberg.Properties, key string) (int64, error) {
	raw, ok := props[key]
	if !ok || raw == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("metadata: snapshot summary %s: %w", key, err)
	}
	return n, nil
}

func millis(ms int64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}
