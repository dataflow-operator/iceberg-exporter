// Package metadata publishes table gauges from Iceberg metadata JSON and the
// current snapshot manifest list.
//
// The warehouse is read only when the catalog pointer did not already carry
// metadata bytes. Parsed summaries are cached by content id and snapshot id,
// and a metadataLocation distinguishes a rewrite that kept the same snapshot.
// The manifest list is a separate object, also cached by content id and
// snapshot id, so the same snapshot is not read again for another ref or for
// a metadata rewrite that kept that snapshot.
//
// Collect serves the last successful summary from memory. Update performs the
// reads. A failure of one table sets iceberg_table_scrape_error and leaves the
// other tables published. Metadata JSON and the manifest list use different
// stage labels, so a bad manifest list still publishes the metadata gauges.
//
// Manifest entries are a third stage. Update only enqueues a snapshot when its
// file-level cache is missing or older than the refresh interval. QueueStale
// does the same enqueue without reading the warehouse. The exporter calls it
// while rendering /metrics, so the scrape itself does not take the collector
// lock. Workers read
// data-file entries in the background, and delete manifests only when that
// scan is enabled. Collect republishes the histogram, small-file ratio, and
// partition skew from memory, so a scrape does not wait on those reads.
package metadata
