package metadata

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

const (
	propUpdateMode       = "write.update.mode"
	propMergeMode        = "write.merge.mode"
	propDistributionMode = "write.distribution-mode"

	configUnset = "unset"
	configOther = "other"
	configNone  = "none"

	maxPartitionTransforms = 8
)

var transformName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\[[0-9]+\])?$`)

func applyWriteConfig(stats *Stats, meta table.Metadata) {
	props := meta.Properties()
	stats.Format = enumProp(props, table.WriteFormatDefaultKey, "parquet", "avro", "orc")
	stats.DeleteMode = enumProp(props, table.WriteDeleteModeKey, table.WriteModeCopyOnWrite, table.WriteModeMergeOnRead)
	stats.UpdateMode = enumProp(props, propUpdateMode, table.WriteModeCopyOnWrite, table.WriteModeMergeOnRead)
	stats.MergeMode = enumProp(props, propMergeMode, table.WriteModeCopyOnWrite, table.WriteModeMergeOnRead)
	stats.DistributionMode = enumProp(props, propDistributionMode, "none", "hash", "range")
	stats.Compression = enumProp(props, table.ParquetCompressionKey,
		"zstd", "snappy", "gzip", "lz4", "uncompressed", "brotli", "lzo", "none")
	stats.TargetFileSize, stats.TargetFileSizeSet = positiveProp(props, table.WriteTargetFileSizeBytesKey)
	stats.RowGroupSize, stats.RowGroupSizeSet = positiveProp(props, table.ParquetRowGroupSizeBytesKey)
	stats.PageSize, stats.PageSizeSet = positiveProp(props, table.ParquetPageSizeBytesKey)
	stats.ManifestTarget, stats.ManifestTargetSet = positiveProp(props, table.ManifestTargetSizeBytesKey)

	spec := meta.PartitionSpec()
	stats.PartitionSpecID = spec.ID()
	stats.PartitionFields = spec.NumFields()
	stats.PartitionTransforms = formatTransforms(spec)
}

func enumProp(props iceberg.Properties, key string, allowed ...string) string {
	raw, ok := props[key]
	if !ok {
		return configUnset
	}
	value := strings.ToLower(strings.TrimSpace(raw))
	if value == "" {
		return configUnset
	}
	for _, name := range allowed {
		if value == name {
			return value
		}
	}
	return configOther
}

func positiveProp(props iceberg.Properties, key string) (int64, bool) {
	raw, ok := props[key]
	if !ok {
		return 0, false
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n <= 0 {
		return 0, false
	}
	return n, true
}

func formatTransforms(spec iceberg.PartitionSpec) string {
	n := spec.NumFields()
	if n == 0 {
		return configNone
	}
	limit := n
	truncated := false
	if limit > maxPartitionTransforms {
		limit = maxPartitionTransforms
		truncated = true
	}
	parts := make([]string, 0, limit+1)
	for i := 0; i < limit; i++ {
		name := spec.Field(i).Transform.String()
		if !transformName.MatchString(name) {
			name = configOther
		}
		parts = append(parts, name)
	}
	if truncated {
		parts = append(parts, "+")
	}
	return strings.Join(parts, ",")
}
