package metadata

import (
	"strconv"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
)

func TestParseWriteConfig(t *testing.T) {
	t.Parallel()
	raw := metadataJSON(metadataOpts{records: 1, operation: "append"})
	raw = withProperties(t, raw, `
		"write.format.default": "PARQUET",
		"write.delete.mode": "merge-on-read",
		"write.update.mode": "copy-on-write",
		"write.merge.mode": "copy-on-write",
		"write.distribution-mode": "hash",
		"write.parquet.compression-codec": "ZSTD",
		"write.target-file-size-bytes": "536870912",
		"write.parquet.row-group-size-bytes": "nope",
		"write.parquet.page-size-bytes": "0",
		"commit.manifest.target-size-bytes": "8388608",
		"write.metadata.path": "s3://wh/custom"
	`)
	got, err := Parse(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	s := got.Stats
	if s.Format != "parquet" || s.DeleteMode != "merge-on-read" || s.UpdateMode != "copy-on-write" || s.MergeMode != "copy-on-write" {
		t.Fatalf("modes %+v", s)
	}
	if s.DistributionMode != "hash" || s.Compression != "zstd" {
		t.Fatalf("format settings %+v", s)
	}
	if !s.TargetFileSizeSet || s.TargetFileSize != 536870912 {
		t.Fatalf("target %+v set=%v", s.TargetFileSize, s.TargetFileSizeSet)
	}
	if s.RowGroupSizeSet || s.PageSizeSet {
		t.Fatalf("invalid sizes were kept: row=%v page=%v", s.RowGroupSizeSet, s.PageSizeSet)
	}
	if !s.ManifestTargetSet || s.ManifestTarget != 8388608 {
		t.Fatalf("manifest target %+v", s.ManifestTarget)
	}
	if s.PartitionFields != 0 || s.PartitionSpecID != 0 || s.PartitionTransforms != "none" {
		t.Fatalf("empty spec %+v %q", s.PartitionFields, s.PartitionTransforms)
	}
}

func TestParseWriteConfigUnset(t *testing.T) {
	t.Parallel()
	got, err := Parse(metadataJSON(metadataOpts{records: 1, operation: "append"}))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	s := got.Stats
	if s.Format != "unset" || s.DeleteMode != "unset" || s.UpdateMode != "unset" || s.MergeMode != "unset" || s.DistributionMode != "unset" || s.Compression != "unset" {
		t.Fatalf("expected unset labels, got %+v", s)
	}
	if s.TargetFileSizeSet || s.RowGroupSizeSet || s.PageSizeSet || s.ManifestTargetSet {
		t.Fatal("unset numeric properties were published")
	}
}

func TestParseWriteConfigUnknownValues(t *testing.T) {
	t.Parallel()
	raw := withProperties(t, metadataJSON(metadataOpts{records: 1, operation: "append"}), `
		"write.format.default": "csv",
		"write.delete.mode": "copy_on_write",
		"write.distribution-mode": "fanout",
		"write.parquet.compression-codec": "xz"
	`)
	got, err := Parse(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	s := got.Stats
	if s.Format != "other" || s.DeleteMode != "other" || s.DistributionMode != "other" || s.Compression != "other" {
		t.Fatalf("unknown values %+v", s)
	}
	if s.UpdateMode != "unset" {
		t.Fatalf("missing update mode %q", s.UpdateMode)
	}
}

func TestParsePartitionSpec(t *testing.T) {
	t.Parallel()
	raw := metadataJSON(metadataOpts{records: 1, operation: "append"})
	raw = replacePartitionSpec(t, raw, `{
		"spec-id": 3,
		"fields": [
			{"name": "ts_day", "transform": "day", "source-id": 1, "field-id": 1000},
			{"name": "id_bucket", "transform": "bucket[16]", "source-id": 1, "field-id": 1001}
		]
	}`, 3, 1001)
	got, err := Parse(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	s := got.Stats
	if s.PartitionSpecID != 3 || s.PartitionFields != 2 || s.PartitionTransforms != "day,bucket[16]" {
		t.Fatalf("spec id=%d fields=%d transforms=%q", s.PartitionSpecID, s.PartitionFields, s.PartitionTransforms)
	}
}

func TestFormatTransformsTruncatesAndRejectsUnknown(t *testing.T) {
	t.Parallel()
	fields := make([]iceberg.PartitionField, 9)
	for i := range fields {
		fields[i] = iceberg.PartitionField{
			SourceIDs: []int{1},
			FieldID:   1000 + i,
			Name:      "p",
			Transform: iceberg.IdentityTransform{},
		}
	}
	spec := iceberg.NewPartitionSpecID(4, fields...)
	got := formatTransforms(spec)
	want := "identity,identity,identity,identity,identity,identity,identity,identity,+"
	if got != want {
		t.Fatalf("transforms %q", got)
	}
	weird := iceberg.NewPartitionSpecID(1, iceberg.PartitionField{
		SourceIDs: []int{1},
		FieldID:   1000,
		Name:      "p",
		Transform: iceberg.UnknownTransform{},
	})
	if got := formatTransforms(weird); got != "other" {
		t.Fatalf("unknown transform %q", got)
	}
}

func TestParseEmptyTableKeepsWriteConfig(t *testing.T) {
	t.Parallel()
	raw := withProperties(t, emptyMetadataJSON(1), `"write.delete.mode": "copy-on-write"`)
	got, err := Parse(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got.Stats.HasSnapshot || got.Stats.DeleteMode != "copy-on-write" || got.Stats.PartitionTransforms != "none" {
		t.Fatalf("empty table config %+v", got.Stats)
	}
}

func withProperties(t *testing.T, raw []byte, props string) []byte {
	t.Helper()
	next := strings.Replace(string(raw), `"properties": {}`, `"properties": {`+props+`}`, 1)
	if next == string(raw) {
		t.Fatal("properties object was not replaced")
	}
	return []byte(next)
}

func replacePartitionSpec(t *testing.T, raw []byte, spec string, specID, lastID int) []byte {
	t.Helper()
	body := string(raw)
	body = strings.Replace(body, `"partition-specs": [{"spec-id": 0, "fields": []}]`, `"partition-specs": [`+spec+`]`, 1)
	body = strings.Replace(body, `"default-spec-id": 0`, `"default-spec-id": `+strconv.Itoa(specID), 1)
	body = strings.Replace(body, `"last-partition-id": 999`, `"last-partition-id": `+strconv.Itoa(lastID), 1)
	if body == string(raw) {
		t.Fatal("partition spec was not replaced")
	}
	return []byte(body)
}
