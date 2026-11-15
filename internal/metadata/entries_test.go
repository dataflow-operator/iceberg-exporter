package metadata

import (
	"testing"

	"github.com/apache/iceberg-go"
)

func TestAccumulatorSkewAndSmallFiles(t *testing.T) {
	t.Parallel()
	acc := newAccumulator()
	spec := idPartitionSpec()
	const threshold = int64(500)
	acc.addEntry(dataEntry(t, iceberg.EntryStatusADDED, spec, "a", 100, 1, 1), iceberg.ManifestContentData, 0, threshold)
	acc.addEntry(dataEntry(t, iceberg.EntryStatusEXISTING, spec, "b", 300, 1, 1), iceberg.ManifestContentData, 0, threshold)
	acc.addEntry(dataEntry(t, iceberg.EntryStatusADDED, spec, "c", 1000, 4, 2), iceberg.ManifestContentData, 0, threshold)
	acc.addEntry(dataEntry(t, iceberg.EntryStatusDELETED, spec, "d", 5000, 9, 3), iceberg.ManifestContentData, 0, threshold)

	agg := acc.finish(false)
	if agg.files != 3 || agg.count != 3 || agg.sum != 1400 {
		t.Fatalf("totals %+v", agg)
	}
	if agg.small != 2 {
		t.Fatalf("small files = %d, want 2", agg.small)
	}
	if agg.partitions != 2 || agg.maxBytes != 1000 {
		t.Fatalf("partitions=%d max=%d", agg.partitions, agg.maxBytes)
	}
	const wantSkew = float64(1000) * 2 / 1400
	if agg.skew != wantSkew {
		t.Fatalf("skew = %v, want %v", agg.skew, wantSkew)
	}
	if agg.partitions == 0 || len(acc.parts) != 0 {
		t.Fatal("partition map was kept after the aggregate")
	}
	for _, n := range agg.buckets {
		if n != 3 {
			t.Fatalf("buckets = %v, want every bound to include all 3 files", agg.buckets)
		}
	}
}

func TestAccumulatorBucketsAndExactThreshold(t *testing.T) {
	t.Parallel()
	acc := newAccumulator()
	spec := iceberg.NewPartitionSpecID(0)
	threshold := DefaultSmallFileBytes
	sizes := []int64{
		8 << 20,
		(8 << 20) + 1,
		1 << 30,
		(1 << 30) + 1,
	}
	for _, size := range sizes {
		acc.addEntry(
			dataEntry(t, iceberg.EntryStatusADDED, spec, "f", size, 1, 0),
			iceberg.ManifestContentData, 0, threshold,
		)
	}
	// Exactly the threshold is not small. One byte under is.
	acc.addEntry(dataEntry(t, iceberg.EntryStatusEXISTING, spec, "edge", threshold, 1, 0), iceberg.ManifestContentData, 0, threshold)
	acc.addEntry(dataEntry(t, iceberg.EntryStatusADDED, spec, "under", threshold-1, 1, 0), iceberg.ManifestContentData, 0, threshold)

	agg := acc.finish(false)
	if agg.files != 6 || agg.small != 3 {
		t.Fatalf("files=%d small=%d, want 6 and 3", agg.files, agg.small)
	}
	if agg.partitions != 1 || agg.skew != 1 {
		t.Fatalf("unpartitioned skew %+v", agg)
	}
	// 8MiB holds only the exact 8MiB file. 32MiB also holds 8MiB+1, 32MiB-1, and 32MiB.
	// 1GiB adds the exact 1GiB file. 1GiB+1 is only in the sample count.
	want := []uint64{1, 4, 4, 4, 5}
	for i, n := range want {
		if agg.buckets[i] != n {
			t.Fatalf("bucket %v = %d, want %d in %v", fileSizeBucketBounds[i], agg.buckets[i], n, agg.buckets)
		}
	}
	if agg.count != 6 {
		t.Fatalf("count = %d", agg.count)
	}
}

func TestAccumulatorDeleteBytesStayOutOfTheHistogram(t *testing.T) {
	t.Parallel()
	acc := newAccumulator()
	spec := iceberg.NewPartitionSpecID(0)
	acc.addEntry(deleteEntry(t, iceberg.EntryContentPosDeletes, 10), iceberg.ManifestContentDeletes, 0, 32)
	acc.addEntry(deleteEntry(t, iceberg.EntryContentEqDeletes, 25), iceberg.ManifestContentDeletes, 0, 32)
	deleted := deleteEntry(t, iceberg.EntryContentEqDeletes, 100)
	acc.addEntry(
		iceberg.NewManifestEntry(iceberg.EntryStatusDELETED, snapshotPtr(), seqPtr(), seqPtr(), deleted.DataFile()),
		iceberg.ManifestContentDeletes, 0, 32,
	)
	acc.addEntry(dataEntry(t, iceberg.EntryStatusADDED, spec, "data", 40, 1, 0), iceberg.ManifestContentData, 0, 32)

	agg := acc.finish(true)
	if agg.positionBytes != 10 || agg.equalityBytes != 25 {
		t.Fatalf("delete bytes %+v", agg)
	}
	if agg.files != 1 || agg.sum != 40 || agg.partitions != 1 {
		t.Fatalf("data aggregate %+v", agg)
	}
}

func TestPartitionKeySeparatesSpecs(t *testing.T) {
	t.Parallel()
	acc := newAccumulator()
	spec := idPartitionSpec()
	acc.addEntry(dataEntry(t, iceberg.EntryStatusADDED, spec, "a", 10, 1, 7), iceberg.ManifestContentData, 0, 100)
	acc.addEntry(dataEntry(t, iceberg.EntryStatusADDED, spec, "b", 10, 1, 7), iceberg.ManifestContentData, 1, 100)
	agg := acc.finish(false)
	if agg.partitions != 2 || agg.maxBytes != 10 || agg.skew != 1 {
		t.Fatalf("spec split %+v", agg)
	}
}

func TestEmptyAggregateIsZero(t *testing.T) {
	t.Parallel()
	acc := newAccumulator()
	agg := acc.finish(true)
	if agg.files != 0 || agg.partitions != 0 || agg.skew != 0 || agg.maxBytes != 0 || agg.sum != 0 {
		t.Fatalf("empty %+v", agg)
	}
	if len(agg.buckets) != len(fileSizeBucketBounds) {
		t.Fatalf("buckets %v", agg.buckets)
	}
	for _, n := range agg.buckets {
		if n != 0 {
			t.Fatalf("buckets %v", agg.buckets)
		}
	}
}

func newAccumulator() accumulator {
	return accumulator{
		parts:   make(map[string]*partBytes),
		buckets: make([]uint64, len(fileSizeBucketBounds)),
	}
}

func idPartitionSpec() iceberg.PartitionSpec {
	return iceberg.NewPartitionSpecID(0, iceberg.PartitionField{
		SourceIDs: []int{1},
		FieldID:   1000,
		Name:      "id",
		Transform: iceberg.IdentityTransform{},
	})
}

func dataEntry(t *testing.T, status iceberg.ManifestEntryStatus, spec iceberg.PartitionSpec, path string, size, records int64, part int64) iceberg.ManifestEntry {
	t.Helper()
	values := map[int]any{}
	if !spec.IsUnpartitioned() {
		values[1000] = part
	}
	return fileEntry(t, status, spec, iceberg.EntryContentData, path, size, records, values)
}

func deleteEntry(t *testing.T, content iceberg.ManifestEntryContent, size int64) iceberg.ManifestEntry {
	t.Helper()
	return fileEntry(t, iceberg.EntryStatusADDED, iceberg.NewPartitionSpecID(0), content, "delete.parquet", size, 1, map[int]any{})
}

func fileEntry(t *testing.T, status iceberg.ManifestEntryStatus, spec iceberg.PartitionSpec, content iceberg.ManifestEntryContent, path string, size, records int64, part map[int]any) iceberg.ManifestEntry {
	t.Helper()
	builder, err := iceberg.NewDataFileBuilder(
		spec, content, path, iceberg.ParquetFile, part, map[int]string{}, map[int]int{}, records, size,
	)
	if err != nil {
		t.Fatalf("data file: %v", err)
	}
	if content == iceberg.EntryContentEqDeletes {
		builder.EqualityFieldIDs([]int{1})
	}
	return iceberg.NewManifestEntry(status, snapshotPtr(), seqPtr(), seqPtr(), builder.Build())
}

func snapshotPtr() *int64 {
	id := int64(7)
	return &id
}

func seqPtr() *int64 {
	seq := int64(1)
	return &seq
}
