package metadata

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/apache/iceberg-go"
)

// ManifestCounts is the current snapshot manifest list, without manifest entries.
// PartitionSpecs is the number of distinct partition spec ids in that list.
type ManifestCounts struct {
	Data           int
	Deletes        int
	PartitionSpecs int
}

// ParseManifestList reads one manifest-list Avro file.
// An empty list is valid and yields zero counts.
func ParseManifestList(raw []byte) (ManifestCounts, error) {
	if len(raw) == 0 {
		return ManifestCounts{}, errors.New("manifest list: empty file")
	}
	files, err := iceberg.ReadManifestList(bytes.NewReader(raw))
	if err != nil {
		return ManifestCounts{}, fmt.Errorf("manifest list: parse: %w", err)
	}
	counts, _, err := countManifests(files)
	return counts, err
}

func parseManifestSnapshot(raw []byte) (manifestSnapshot, error) {
	if len(raw) == 0 {
		return manifestSnapshot{}, errors.New("manifest list: empty file")
	}
	files, err := iceberg.ReadManifestList(bytes.NewReader(raw))
	if err != nil {
		return manifestSnapshot{}, fmt.Errorf("manifest list: parse: %w", err)
	}
	counts, objects, err := countManifests(files)
	if err != nil {
		return manifestSnapshot{}, err
	}
	return manifestSnapshot{counts: counts, files: objects}, nil
}

func countManifests(files []iceberg.ManifestFile) (ManifestCounts, []manifestObject, error) {
	var counts ManifestCounts
	specs := make(map[int32]struct{})
	objects := make([]manifestObject, 0, len(files))
	for _, file := range files {
		switch file.ManifestContent() {
		case iceberg.ManifestContentData:
			counts.Data++
		case iceberg.ManifestContentDeletes:
			counts.Deletes++
		default:
			return ManifestCounts{}, nil, fmt.Errorf("manifest list: content %d", file.ManifestContent())
		}
		specs[file.PartitionSpecID()] = struct{}{}
		objects = append(objects, manifestObject{
			path:    file.FilePath(),
			content: file.ManifestContent(),
			specID:  file.PartitionSpecID(),
			length:  file.Length(),
			version: file.Version(),
		})
	}
	counts.PartitionSpecs = len(specs)
	return counts, objects, nil
}

// embeddedManifestCounts counts v1 manifest paths.
// Those paths do not carry a spec id or a content type. Every v1 manifest
// tracks data files. Spec ids fall back to the specs defined in metadata,
// which is the only number the JSON has.
func embeddedManifestCounts(manifests, metadataSpecs int) ManifestCounts {
	counts := ManifestCounts{Data: manifests}
	if manifests > 0 {
		counts.PartitionSpecs = metadataSpecs
	}
	return counts
}
