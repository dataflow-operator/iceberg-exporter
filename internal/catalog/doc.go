// Package catalog is the Iceberg catalog port used by the exporter.
//
// Adapters implement Catalog. Revision checks, commit diffs, commit history,
// and multiple refs are optional capabilities (Revisioned, Diffable,
// Historian, MultiRef). Refresh
// never branches on the adapter type string: a matching revision token skips
// listing, a diff applies when the adapter can produce one, and every other
// path lists pointers and compares metadataLocation. SnapshotID is only a
// secondary check when both sides have it.
//
// The stable cache key is the catalog content id. Catalogs that do not have
// one use the Iceberg table-uuid once metadata JSON is available, and
// namespace plus name until then. The cache rekeys when the stable id appears.
package catalog
