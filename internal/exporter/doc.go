// Package exporter serves Prometheus /metrics from memory.
//
// Refresh probes each catalog, applies the catalog cache policy, and loads
// table metadata for the published pointers. The HTTP handler does not call
// the catalog or the warehouse. It republishes the last snapshot and, when a
// file-level cache is stale, only enqueues a manifest-entry scan.
//
// A failed probe sets iceberg_catalog_up to 0 and leaves the previous table
// series in place. A failed read of one table sets iceberg_table_scrape_error
// for that table and does not drop the others or fail the scrape.
package exporter
