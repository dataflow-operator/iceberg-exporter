// Package exporter serves Prometheus /metrics from memory.
//
// Refresh probes each catalog, applies the catalog cache policy, and loads
// table metadata for the published pointers. At the end of a successful
// refresh it renders the exposition. The HTTP handler writes that text and
// does not call the catalog, the warehouse, or the registry. A stale
// file-level cache is enqueued during that render, not during the scrape.
//
// A failed probe sets iceberg_catalog_up to 0 and leaves the previous table
// series in place. A failed read of one table sets iceberg_table_scrape_error
// for that table and does not drop the others or fail the scrape.
package exporter
